#include <mutex>
#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/comparator.h>
#include <rocksdb/compaction_filter.h>
#include <topling/side_plugin_factory.h>
#include <dcompact/dcompact_executor.h>
#include <boost/core/demangle.hpp>

/*
#include "debug.h"
#undef  Trace
#undef  Debug
#define Trace(M, ...) {}
#define Debug(M, ...) {}
*/
#include "redis.h"
#include "custom_comparator.h"
#include "base_filter.h"
#include "lists_filter.h"
#include "zsets_filter.h"
#include "strings_filter.h"

namespace blackwidow {

using namespace rocksdb;
using namespace terark;

// prefix "%s" -- this "%s" is for following "", it is a placeholder
// for empty __VA_ARGS__
#define PrintLog(level, prefix, fmt, ...) \
  do { if (SidePluginRepo::DebugLevel() >= level) \
    fprintf(stderr, prefix "%s" fmt "\n", \
            TERARK_PP_SmartForPrintf("", ## __VA_ARGS__)); \
  } while (0)
#define TRAC(...) PrintLog(4, "TRAC: ", __VA_ARGS__)
#define DEBG(...) PrintLog(3, "DEBG: ", __VA_ARGS__)
#define INFO(...) PrintLog(2, "INFO: ", __VA_ARGS__)
#define WARN(...) PrintLog(1, "WARN: ", __VA_ARGS__)

ROCKSDB_REG_DEFAULT_CONS( BaseMetaFilterFactory, CompactionFilterFactory);
ROCKSDB_REG_DEFAULT_CONS(ListsMetaFilterFactory, CompactionFilterFactory);
ROCKSDB_REG_DEFAULT_CONS(  StringsFilterFactory, CompactionFilterFactory);

template<class Base>
struct FilterFac : public Base {
  std::string m_type;
  std::mutex m_mtx;
  const SidePluginRepo* m_repo;
  using Base::Name;
  FilterFac(const json& js, const SidePluginRepo& repo) {
    m_repo = &repo;
    std::string type;
    ROCKSDB_JSON_REQ_PROP(js, type);
    m_type = type;
  }
  bool TrySetDBptr() {
    if (this->db_ptr_ && this->cf_handles_ptr_) {
      return true;
    }
    std::lock_guard<std::mutex> lock(m_mtx);
    if (!this->db_ptr_) {
      //DB_MultiCF* dbm = (*m_repo)[m_type]; // this line crashes gdb
      DB_Ptr dbp(nullptr);
      if (m_repo->Get(m_type, &dbp)) {
        if (!(dbp.db && dbp.dbm)) {
          INFO("DB(%s) is opening 1: %s::CreateCompactionFilter()", m_type, Name());
          return false; // db is in opening
        }
        DB_MultiCF* dbm = dbp.dbm;
        this->db_ptr_ = &dbm->db;
        this->cf_handles_ptr_ = &dbm->cf_handles;
      }
      else {
        INFO("DB(%s) is opening 2: %s::CreateCompactionFilter()", m_type, Name());
        return false;
      }
    }
    return true;
  }
  virtual std::unique_ptr<rocksdb::CompactionFilter>
  CreateCompactionFilter(const CompactionFilterContext& context) final {
    if (!IsCompactionWorker()) {
      TrySetDBptr();
    }
    return Base::CreateCompactionFilter(context);
  }
};
#define BW_RegFilterFac(Class) using Class##JS = FilterFac<Class>; \
  ROCKSDB_REG_JSON_REPO_CONS(#Class, Class##JS, CompactionFilterFactory)

BW_RegFilterFac(  BaseDataFilterFactory);
BW_RegFilterFac( ListsDataFilterFactory);
BW_RegFilterFac(ZSetsScoreFilterFactory);

const rocksdb::Comparator*  ListsDataKeyComparator();
const rocksdb::Comparator* ZSetsScoreKeyComparator();
static const rocksdb::Comparator*
JS_ListsDataKeyComparator(const json&, const SidePluginRepo&) {
  return ListsDataKeyComparator();
}
static const rocksdb::Comparator*
JS_ZSetsScoreKeyComparator(const json&, const SidePluginRepo&) {
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
    DB** dbp = fac->db_ptr_;
    auto filter = new HosterFilter(dbp ? *dbp : NULL, fac->cf_handles_ptr_);
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
                 int job_id,
                 const std::string& type,
                 const CompactionFilterFactory& fac,
                 DB** dbpp, std::vector<ColumnFamilyHandle*>* cfh_vec,
                 Slice smallest_user_key, Slice largest_user_key,
                 VersionTimestamp (*decode)(std::string*))
{
  DB* db = *dbpp;
  ColumnFamilyHandle* cfh = (*cfh_vec)[0];
  std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions(), cfh));
  const std::string start = decode_00_0n(smallest_user_key);
  const std::string bound = decode_00_0n(largest_user_key);
  using namespace std::chrono;
  auto t0 = steady_clock::now();
  if (start.empty()) {
    iter->SeekToFirst();
  } else {
    iter->Seek(start);
  }
  std::string meta_value;
  size_t bytes = 0;
  while (iter->Valid()) {
    Slice k = iter->key(); if (!bound.empty() && bound < k) break;
    Slice v = iter->value();
    meta_value.assign(v.data_, v.size_);
    ttlmap[k] = decode(&meta_value);
    bytes += k.size_ + 1; // 1 for key len byte, for simple, ignore len > 255
    iter->Next();
  }
  bytes += sizeof(VersionTimestamp) * ttlmap.size();
  auto t1 = steady_clock::now();
  double d = duration_cast<microseconds>(t1-t0).count()/1e6;
  INFO("job_id: %d: %s.%s.Serialize: %8.4f sec %8.6f Mkv %9.6f MB, start = %s, bound = %s",
        job_id, type, fac.Name(), d, ttlmap.size()/1e6, bytes/1e6, start, bound);
}

template<class Factory, class ParsedMetaValue>
struct DataFilterFactorySerDe : SerDeFunc<CompactionFilterFactory> {
  using ConcreteFactory = FilterFac<Factory>;
  std::string smallest_user_key, largest_user_key;
  int job_id;
  DataFilterFactorySerDe(const json& js, const SidePluginRepo& repo) {
    auto cp = JS_CompactionParamsDecodePtr(js);
    smallest_user_key = cp->smallest_user_key;
    largest_user_key = cp->largest_user_key;
    job_id = cp->job_id;
    TRAC("%s: job_id = %d, smallest_user_key = %s, largest_user_key = %s",
        boost::core::demangle(typeid(DataFilterFactorySerDe).name()).c_str(),
        cp->job_id, smallest_user_key.c_str(), largest_user_key.c_str());
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
      if (const_cast<ConcreteFactory&>(fac).TrySetDBptr()) {
        load_ttlmap(ttlmap, job_id, fac.m_type, fac, fac.db_ptr_, fac.cf_handles_ptr_,
            smallest_user_key, largest_user_key, &DecodeVT<ParsedMetaValue>);
      }
      else {
        // now it is in DB Open, do not load ttlmap
        DEBG("job_id: %d: %s.%s.Serialize: db is in opening",
              job_id, fac.m_type.c_str(), fac.Name());
      }
      int64_t unix_time;
      rocksdb::Env::Default()->GetCurrentTime(&unix_time);
      auto pos0 = dio.tell();
      dio << unix_time;
      dio << ttlmap;
      auto kvs = fac.ttlmap_.size();
      auto pos1 = dio.tell();
      auto bytes = size_t(pos1-pos0);
      DEBG("job_id: %d: %s.%s.Serialize: kvs = %zd, bytes = %zd",
            job_id, fac.m_type.c_str(), fac.Name(), kvs, bytes);
    }
  }
  void DeSerialize(FILE* reader, CompactionFilterFactory* base)
  const override {
    auto fac = dynamic_cast<ConcreteFactory*>(base);
    if (IsCompactionWorker()) {
      LittleEndianDataInput<NonOwnerFileStream> dio(reader);
      auto pos0 = dio.tell();
      dio >> fac->unix_time_;
      dio >> fac->ttlmap_;
      auto kvs = fac->ttlmap_.size();
      auto pos1 = dio.tell();
      auto bytes = size_t(pos1-pos0);
      DEBG("job_id: %d: %s.%s.DeSerialize: kvs = %zd, bytes = %zd",
            job_id, fac->m_type.c_str(), fac->Name(), kvs, bytes);
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


struct HtmlTextDecode : public UserKeyCoder {
  void Update(const json&, const SidePluginRepo&) override {
  }
  std::string ToString(const json&, const SidePluginRepo&) const override {
    return "This is the HtmlTextDecode.";
  }
  void Encode(Slice, std::string*) const override {
    assert(!"Unexpected call");
    THROW_InvalidArgument("Unexpected call");
  }
  void Decode(Slice coded, std::string* de) const override {
    std::string tmp_s;
    blackwidow::ParsedBaseDataKey tmp_p(coded, &tmp_s);
    auto k = tmp_p.key();
    auto d = tmp_p.data();
    de->clear();
    de->reserve(coded.size_);
    HtmlAppendEscape(de, k.data(), k.size());
    de->append(":");
    de->append(std::to_string(tmp_p.version()));
    de->append(":");
    HtmlAppendEscape(de, d.data(), d.size());
  }
};
ROCKSDB_REG_DEFAULT_CONS(HtmlTextDecode, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("HtmlTextDecode");


} // namespace blackwidow
