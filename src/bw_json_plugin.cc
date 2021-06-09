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
        ROCKSDB_VERIFY_F(nullptr != dbm, "type = %s", m_type.c_str());
        this->db_ptr_ = &dbm->db;
        this->cf_handles_ptr_ = &dbm->cf_handles;
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
      std::unique_ptr<rocksdb::Iterator> iter((*fac.db_ptr_)->
            NewIterator(ReadOptions(), (*fac.cf_handles_ptr_)[0]));
      iter->Seek(smallest_user_key);
      hash_strmap<VersionTimestamp> ttlmap;
      std::string meta_value;
      while (iter->Valid()) {
        Slice k = iter->key(); if (largest_user_key < k) break;
        Slice v = iter->value();
        meta_value.assign(v.data_, v.size_);
        ParsedMetaValue parsed_meta_value(&meta_value);
        auto& vt = ttlmap[k];
        vt.version = parsed_meta_value.version();
        vt.timestamp = parsed_meta_value.timestamp();
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

char* encode_00_0n(const char* ibeg, const char* iend, char* obeg, char* oend, char out_end_mark) {
  ROCKSDB_VERIFY(0 != out_end_mark);
  for (; ibeg < iend; ++ibeg) {
    ROCKSDB_VERIFY_F(obeg < oend, "broken data: input remain bytes = %zd",
                     iend - ibeg);
    char b = *ibeg;
    if (LIKELY(0 != b)) {
      *obeg++ = b;
    }
    else {
      obeg[0] = obeg[1] = 0; // 0 -> 00
      obeg += 2;
    }
  }
  obeg[0] = 0;
  obeg[1] = out_end_mark;
  return obeg + 2;
}

char* decode_00_0n(const char* ibeg, const char** ires, char* obeg, char* oend) {
  const char* icur = ibeg;
  for (; ; ++obeg) {
    ROCKSDB_VERIFY_F(obeg < oend, "broken data: decoded input bytes = %zd",
                     icur - ibeg);
    char b = *icur;
    if (LIKELY(0 != b)) {
      *obeg = b;
      icur++;
    }
    else {
      b = icur[1];
      if (0 != b) {
        obeg[0] = 0;
        obeg[1] = b; // out_end_mark in encode_00_0n
        break;
      }
      else { // 00 -> 0
        *obeg = 0;
        icur += 2;
      }
    }
  }
  *ires = icur + 1;
  return obeg + 2;
}

const char* end_of_00_0n(const char* encoded) {
  while (true) {
    if (encoded[0])
      encoded++;
    else if (encoded[1]) // 0n
      return encoded + 2;
    else // 00
      encoded += 2;
  }
}

} // namespace blackwidow
