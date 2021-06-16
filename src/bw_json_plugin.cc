#include <mutex>
#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/comparator.h>
#include <rocksdb/compaction_filter.h>
#include <utilities/json/json_plugin_factory.h>
#include "redis.h"
#include "custom_comparator.h"
#include "base_filter.h"
#include "lists_filter.h"
#include "zsets_filter.h"
#include "strings_filter.h"

namespace blackwidow {

using namespace rocksdb;
using namespace terark;

ROCKSDB_REG_DEFAULT_CONS( BaseMetaFilterFactory, CompactionFilterFactory);
ROCKSDB_REG_DEFAULT_CONS(ListsMetaFilterFactory, CompactionFilterFactory);

ROCKSDB_REG_DEFAULT_CONS( StringsFilterFactory, CompactionFilterFactory);

template<class Base>
struct Tpl_FilterFactoryJS : public Base {
  std::string m_type;
  std::mutex m_mtx;
  const JsonPluginRepo* m_repo;
  Tpl_FilterFactoryJS(const json& js, const JsonPluginRepo& repo) {
    m_repo = &repo;
    std::string type;
    ROCKSDB_JSON_REQ_PROP(js, type);
    m_type = type;
  }
  virtual std::unique_ptr<rocksdb::CompactionFilter>
  CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context)
  final {
    if (!IsCompactionWorker()) {
      std::lock_guard<std::mutex> lock(m_mtx);
      if (!this->db_ptr_) {
        DB_MultiCF* dbm = (*m_repo)[m_type];
        if (dbm) {
          this->db_ptr_ = &dbm->db;
          this->cf_handles_ptr_ = &dbm->cf_handles;
        }
        else {
          fprintf(stderr,
              "INFO: DB(%s) is opening: %s::CreateCompactionFilter()\n",
              m_type.c_str(), this->Name());
        }
      }
    }
    return Base::CreateCompactionFilter(context);
  }
};
using   BaseDataFilterFactoryJS = Tpl_FilterFactoryJS<  BaseDataFilterFactory>;
using  ListsDataFilterFactoryJS = Tpl_FilterFactoryJS< ListsDataFilterFactory>;
using ZSetsScoreFilterFactoryJS = Tpl_FilterFactoryJS<ZSetsScoreFilterFactory>;
ROCKSDB_REG_JSON_REPO_CONS(  "BaseDataFilterFactory",
                              BaseDataFilterFactoryJS, CompactionFilterFactory);
ROCKSDB_REG_JSON_REPO_CONS( "ListsDataFilterFactory",
                             ListsDataFilterFactoryJS, CompactionFilterFactory);
ROCKSDB_REG_JSON_REPO_CONS("ZSetsScoreFilterFactory",
                            ZSetsScoreFilterFactoryJS, CompactionFilterFactory);

const rocksdb::Comparator*  ListsDataKeyComparator();
const rocksdb::Comparator* ZSetsScoreKeyComparator();
static const rocksdb::Comparator*
JS_ListsDataKeyComparator(const json&, const JsonPluginRepo&) {
  return ListsDataKeyComparator();
}
static const rocksdb::Comparator*
JS_ZSetsScoreKeyComparator(const json&, const JsonPluginRepo&) {
  return ZSetsScoreKeyComparator();
}

ROCKSDB_FACTORY_REG("blackwidow.ListsDataKeyComparator",
                             JS_ListsDataKeyComparator);
ROCKSDB_FACTORY_REG("blackwidow.ZSetsScoreKeyComparator",
                             JS_ZSetsScoreKeyComparator);

template<class ConcreteFilter, class Factory>
static std::unique_ptr<CompactionFilter> Tpl_SimpleNewFilter(const Factory* fac) {
  auto filter = new ConcreteFilter;
  if (IsCompactionWorker()) {
    filter->unix_time = fac->unix_time_;
  } else {
    rocksdb::Env::Default()->GetCurrentTime(&filter->unix_time);
  }
  return std::unique_ptr<CompactionFilter>(filter);
}

std::unique_ptr<CompactionFilter>
BaseMetaFilterFactory::CreateCompactionFilter(const CompactionFilterContext&) {
  return Tpl_SimpleNewFilter<BaseMetaFilter>(this);
}
std::unique_ptr<CompactionFilter>
StringsFilterFactory::CreateCompactionFilter(const CompactionFilterContext&) {
  return Tpl_SimpleNewFilter<StringsFilter>(this);
}
std::unique_ptr<CompactionFilter>
ListsMetaFilterFactory::CreateCompactionFilter(const CompactionFilterContext&) {
  return Tpl_SimpleNewFilter<ListsMetaFilter>(this);
}

template<class ConcreteFactory>
struct SimpleFilterFactorySerDe : SerDeFunc<CompactionFilterFactory> {
  void Serialize(FILE* output, const CompactionFilterFactory& base)
  const override {
    //auto& fac = dynamic_cast<const ConcreteFactory&>(base);
    LittleEndianDataOutput<NonOwnerFileStream> dio(output);
    if (IsCompactionWorker()) {
      // do nothing
    }
    else {
      int64_t unix_time;
      rocksdb::Env::Default()->GetCurrentTime(&unix_time);
      dio << unix_time;
    }
  }
  void DeSerialize(FILE* reader, CompactionFilterFactory* base)
  const override {
    auto fac = dynamic_cast<ConcreteFactory*>(base);
    if (IsCompactionWorker()) {
      LittleEndianDataInput<NonOwnerFileStream> dio(reader);
      dio >> fac->unix_time_;
    }
    else {
      // do nothing
    }
  }
};
#define RegSimpleFilterFactorySerDe(Factory) \
  typedef SimpleFilterFactorySerDe<Factory> Factory##SerDe; \
  ROCKSDB_REG_PluginSerDe(#Factory, Factory##SerDe)

RegSimpleFilterFactorySerDe(BaseMetaFilterFactory);
RegSimpleFilterFactorySerDe(ListsMetaFilterFactory);
RegSimpleFilterFactorySerDe(StringsFilterFactory);

class WorkerBaseDataFilter : public BaseDataFilter {
public:
  WorkerBaseDataFilter() : BaseDataFilter(nullptr, nullptr) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    ParsedBaseDataKey parsed_base_data_key(key, &parse_key_buf_);
    Trace("==========================START==========================");
    Trace("[DataFilter], key: %s, data = %s, version = %d",
          parsed_base_data_key.key().ToString().c_str(),
          parsed_base_data_key.data().ToString().c_str(),
          parsed_base_data_key.version());

    Slice new_cur_key = parsed_base_data_key.key();
    if (new_cur_key != cur_key_) {
      cur_key_.assign(new_cur_key.data_, new_cur_key.size_);
      size_t idx = ttlmap_->find_i(cur_key_);
      if (ttlmap_->end_i() != idx) {
        meta_not_found_ = false;
        cur_meta_version_ = ttlmap_->val(idx).version;
        cur_meta_timestamp_ = ttlmap_->val(idx).timestamp;
      } else {
        meta_not_found_ = true;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      return true;
    }

    if (cur_meta_timestamp_ != 0
      && cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[Timeout]");
      return true;
    }

    if (cur_meta_version_ > parsed_base_data_key.version()) {
      Trace("Drop[data_key_version < cur_meta_version]");
      return true;
    } else {
      Trace("Reserve[data_key_version == cur_meta_version]");
      return false;
    }
  }
  const terark::hash_strmap<VersionTimestamp>* ttlmap_;
};

class WorkerListsDataFilter : public ListsDataFilter {
public:
  WorkerListsDataFilter() : ListsDataFilter(nullptr, nullptr) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    ParsedListsDataKey parsed_lists_data_key(key, &parse_key_buf_);
    Trace("==========================START==========================");
    Trace("[DataFilter], key: %s, index = %lu, data = %s, version = %d",
          parsed_lists_data_key.key().ToString().c_str(),
          parsed_lists_data_key.index(),
          value.ToString().c_str(),
          parsed_lists_data_key.version());

    Slice new_cur_key = parsed_lists_data_key.key();
    if (new_cur_key != cur_key_) {
      cur_key_.assign(new_cur_key.data_, new_cur_key.size_);
      size_t idx = ttlmap_->find_i(cur_key_);
      if (ttlmap_->end_i() != idx) {
        meta_not_found_ = false;
        cur_meta_version_ = ttlmap_->val(idx).version;
        cur_meta_timestamp_ = ttlmap_->val(idx).timestamp;
      } else {
        meta_not_found_ = true;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      return true;
    }

    if (cur_meta_timestamp_ != 0
      && cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[Timeout]");
      return true;
    }

    if (cur_meta_version_ > parsed_lists_data_key.version()) {
      Trace("Drop[list_data_key_version < cur_meta_version]");
      return true;
    } else {
      Trace("Reserve[list_data_key_version == cur_meta_version]");
      return false;
    }
  }
  const terark::hash_strmap<VersionTimestamp>* ttlmap_;
};

class WorkerZSetsScoreFilter : public ZSetsScoreFilter {
public:
  WorkerZSetsScoreFilter() : ZSetsScoreFilter(nullptr, nullptr) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
    ParsedZSetsScoreKey parsed_zsets_score_key(key, &parse_key_buf_);
    Trace("==========================START==========================");
    Trace("[ScoreFilter], key: %s, score = %lf, member = %s, version = %d",
          parsed_zsets_score_key.key().ToString().c_str(),
          parsed_zsets_score_key.score(),
          parsed_zsets_score_key.member().ToString().c_str(),
          parsed_zsets_score_key.version());

    Slice new_cur_key = parsed_zsets_score_key.key();
    if (new_cur_key != cur_key_) {
      cur_key_.assign(new_cur_key.data_, new_cur_key.size_);
      size_t idx = ttlmap_->find_i(cur_key_);
      if (ttlmap_->end_i() != idx) {
        meta_not_found_ = false;
        cur_meta_version_ = ttlmap_->val(idx).version;
        cur_meta_timestamp_ = ttlmap_->val(idx).timestamp;
      } else {
        meta_not_found_ = true;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      return true;
    }

    if (cur_meta_timestamp_ != 0 &&
        cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[Timeout]");
      return true;
    }
    if (cur_meta_version_ > parsed_zsets_score_key.version()) {
      Trace("Drop[score_key_version < cur_meta_version]");
      return true;
    } else {
      Trace("Reserve[score_key_version == cur_meta_version]");
      return false;
    }
  }
  const terark::hash_strmap<VersionTimestamp>* ttlmap_;
};

template<class WorkerFilter, class HosterFilter, class Factory>
static std::unique_ptr<CompactionFilter> Tpl_ttlmapNewFilter(const Factory* fac) {
  if (IsCompactionWorker()) {
    auto filter = new WorkerFilter();
    filter->unix_time = fac->unix_time_;
    filter->ttlmap_ = &fac->ttlmap_;
    return std::unique_ptr<CompactionFilter>(filter);
  } else {
    auto filter = new HosterFilter(*fac->db_ptr_, fac->cf_handles_ptr_);
    rocksdb::Env::Default()->GetCurrentTime(&filter->unix_time);
    return std::unique_ptr<CompactionFilter>(filter);
  }
}
std::unique_ptr<CompactionFilter>
BaseDataFilterFactory::CreateCompactionFilter(const CompactionFilterContext&) {
  return Tpl_ttlmapNewFilter<WorkerBaseDataFilter, BaseDataFilter>(this);
}
std::unique_ptr<CompactionFilter>
ListsDataFilterFactory::CreateCompactionFilter(const CompactionFilterContext&) {
  return Tpl_ttlmapNewFilter<WorkerListsDataFilter, ListsDataFilter>(this);
}
std::unique_ptr<CompactionFilter>
ZSetsScoreFilterFactory::CreateCompactionFilter(const CompactionFilterContext&) {
  return Tpl_ttlmapNewFilter<WorkerZSetsScoreFilter, ZSetsScoreFilter>(this);
}

DATA_IO_DUMP_RAW_MEM_E(VersionTimestamp);

std::string decode_00_0n(Slice src) {
  if (src.empty()) {
    return std::string(); // empty
  }
  std::string dst(src.size(), '\0');
  auto src_ptr = src.end();
  auto dst_beg = &dst[0];
  auto dst_end = &dst[0] + dst.size();
  dst_end = decode_00_0n(src.begin(), &src_ptr, dst_beg, dst_end);
  ROCKSDB_VERIFY_LE(size_t(src_ptr - src.begin()), src.size_);
  dst.resize(dst_end - dst_beg);
  return dst;
}

template<class ParsedMetaValue>
static VersionTimestamp DecodeVT(std::string* meta_value) {
  ParsedMetaValue parsed_meta_value(meta_value);
  VersionTimestamp vt;
  vt.version = parsed_meta_value.version();
  vt.timestamp = parsed_meta_value.timestamp();
  return vt;
}

static
void load_ttlmap(hash_strmap<VersionTimestamp>& ttlmap,
                 const CompactionFilterFactory& fac,
                 rocksdb::DB* db, ColumnFamilyHandle* cfh,
                 Slice smallest_user_key, Slice largest_user_key,
                 VersionTimestamp (*decode)(std::string*))
{
  std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions(), cfh));
  const std::string start = decode_00_0n(smallest_user_key);
  const std::string bound = decode_00_0n(largest_user_key);
  fprintf(stderr, "INFO: %s.Serialize: start = %s, bound = %s\n",
          fac.Name(), start.c_str(), bound.c_str());
  if (start.empty()) {
    iter->SeekToFirst();
  } else {
    iter->Seek(start);
  }
  std::string meta_value;
  while (iter->Valid()) {
    Slice k = iter->key(); if (!bound.empty() && bound < k) break;
    Slice v = iter->value();
    meta_value.assign(v.data_, v.size_);
    ttlmap[k] = decode(&meta_value);
    iter->Next();
  }
}

template<class ConcreteFactory, class ParsedMetaValue>
struct DataFilterFactorySerDe : SerDeFunc<CompactionFilterFactory> {
  std::string smallest_user_key, largest_user_key;
  DataFilterFactorySerDe(const json& js, const JsonPluginRepo& repo) {
    ROCKSDB_JSON_REQ_PROP(js, smallest_user_key);
    ROCKSDB_JSON_REQ_PROP(js, largest_user_key);
  }
  void Serialize(FILE* output, const CompactionFilterFactory& base)
  const override {
    auto& fac = dynamic_cast<const ConcreteFactory&>(base);
    LittleEndianDataOutput<NonOwnerFileStream> dio(output);
    if (IsCompactionWorker()) {
      // do nothing
    }
    else {
      hash_strmap<VersionTimestamp> ttlmap;
      if (fac.db_ptr_ && fac.cf_handles_ptr_) {
        load_ttlmap(ttlmap, fac, *fac.db_ptr_, (*fac.cf_handles_ptr_)[0],
            smallest_user_key, largest_user_key, &DecodeVT<ParsedMetaValue>);
      }
      else {
        // now it is in DB Open, do not load ttlmap
        fprintf(stderr, "INFO: %s.Serialize: db is in opening\n", fac.Name());
      }
      int64_t unix_time;
      rocksdb::Env::Default()->GetCurrentTime(&unix_time);
      dio << unix_time;
      dio << ttlmap;
    }
  }
  void DeSerialize(FILE* reader, CompactionFilterFactory* base)
  const override {
    auto fac = dynamic_cast<ConcreteFactory*>(base);
    if (IsCompactionWorker()) {
      LittleEndianDataInput<NonOwnerFileStream> dio(reader);
      dio >> fac->unix_time_;
      dio >> fac->ttlmap_;
    }
    else {
      // do nothing
    }
  }
};

#define RegDataFilterFactorySerDe(Factory, Parsed) \
using Factory##SerDe = DataFilterFactorySerDe<Factory, Parsed>; \
ROCKSDB_REG_JSON_REPO_CONS_3(#Factory, Factory##SerDe, \
                             SerDeFunc<CompactionFilterFactory>)

RegDataFilterFactorySerDe(BaseDataFilterFactory, ParsedBaseMetaValue);
RegDataFilterFactorySerDe(ListsDataFilterFactory, ParsedListsMetaValue);
RegDataFilterFactorySerDe(ZSetsScoreFilterFactory, ParsedZSetsMetaValue);


} // namespace blackwidow
