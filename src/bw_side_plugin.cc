#include <mutex>
#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/process.hpp>
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
#include "src/filter_counter.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


namespace blackwidow {

using namespace rocksdb;
using namespace terark;

#if 0
#define DoPrintLog(...) fprintf(stderr, __VA_ARGS__)
#else
#define DoPrintLog(...) \
    info_log ? ROCKS_LOG_INFO(info_log, __VA_ARGS__) \
             : (void)fprintf(stderr, __VA_ARGS__)
#endif

#define PrintLog(level, fmt, ...) \
  do { if (SidePluginRepo::DebugLevel() >= level) \
    DoPrintLog("%s: " fmt "\n", \
            TERARK_PP_SmartForPrintf(StrDateTimeNow(), ## __VA_ARGS__)); \
  } while (0)
#define TRAC(...) PrintLog(4, "TRAC: " __VA_ARGS__)
#define DEBG(...) PrintLog(3, "DEBG: " __VA_ARGS__)
#define INFO(...) PrintLog(2, "INFO: " __VA_ARGS__)
#define WARN(...) PrintLog(1, "WARN: " __VA_ARGS__)


ROCKSDB_REG_DEFAULT_CONS( BaseMetaFilterFactory, CompactionFilterFactory);
ROCKSDB_REG_DEFAULT_CONS(ListsMetaFilterFactory, CompactionFilterFactory);
ROCKSDB_REG_DEFAULT_CONS(  StringsFilterFactory, CompactionFilterFactory);


BaseDataFilter::~BaseDataFilter() {
  delete iter_;
  factory->local_fl_cnt.add(this->fl_cnt);
}
BaseMetaFilter::~BaseMetaFilter() { factory->local_fl_cnt.add(this->fl_cnt); }
ListsMetaFilter::~ListsMetaFilter() { factory->local_fl_cnt.add(this->fl_cnt); }
ListsDataFilter::~ListsDataFilter() { factory->local_fl_cnt.add(this->fl_cnt); }
StringsFilter::~StringsFilter() { factory->local_fl_cnt.add(this->fl_cnt); }
ZSetsScoreFilter::~ZSetsScoreFilter() { factory->local_fl_cnt.add(this->fl_cnt); }

template<class Base>
struct FilterFac : public Base {
  std::string m_type;
  std::mutex m_mtx;
  const SidePluginRepo* m_repo;
  const CompactionParams* m_cp = nullptr;
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
    Logger* info_log = nullptr;
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
  virtual std::unique_ptr<CompactionFilter>
  CreateCompactionFilter(const CompactionFilterContext&) final;
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
static std::unique_ptr<CompactionFilter> Tpl_SimpleNewFilter(Factory* fac) {
  auto filter = new ConcreteFilter;
  if (IsCompactionWorker()) {
    filter->unix_time = fac->unix_time_;
  } else {
    rocksdb::Env::Default()->GetCurrentTime(&filter->unix_time);
  }
  filter->factory = fac;  // give the Factory pointer to the filer
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


DATA_IO_DUMP_RAW_MEM_E(FilterCounter)

template<class ConcreteFactory>
struct SimpleFilterFactorySerDe : SerDeFunc<CompactionFilterFactory> {
  void Serialize(FILE* output, const CompactionFilterFactory& base)
  const override {
    auto& fac = dynamic_cast<const ConcreteFactory&>(base);
    LittleEndianDataOutput<NonOwnerFileStream> dio(output);
    if (IsCompactionWorker()) {
      dio << fac.local_fl_cnt;
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
    LittleEndianDataInput<NonOwnerFileStream> dio(reader);
    if (IsCompactionWorker()) {
      dio >> fac->unix_time_;
    }
    else {
      FilterCounter temp_fl_cnt;
      dio >> temp_fl_cnt;
      fac->remote_fl_cnt.add(temp_fl_cnt);
    }
  }
};
#define RegSimpleFilterFactorySerDe(Factory) \
  typedef SimpleFilterFactorySerDe<Factory> Factory##SerDe; \
  ROCKSDB_REG_PluginSerDe(#Factory, Factory##SerDe)

RegSimpleFilterFactorySerDe(BaseMetaFilterFactory);
RegSimpleFilterFactorySerDe(ListsMetaFilterFactory);
RegSimpleFilterFactorySerDe(StringsFilterFactory);

DATA_IO_DUMP_RAW_MEM_E(VersionTimestamp);

struct TTL_StreamReader {
  Logger* info_log;
  //OsFileStream m_file;
  ProcPipeStream m_file;
  size_t meta_ttl_num_ = 0;
  size_t meta_ttl_idx_ = 0;
  LittleEndianDataInput<InputBuffer> m_reader;
  std::string mk_from_meta; // mk_ means (meta key)
  VersionTimestamp vt;
  bool ReadUntil(const std::string& mk_from_data) {
    int c = mk_from_meta.compare(mk_from_data);
    while (meta_ttl_idx_ < meta_ttl_num_ && c < 0) {
      m_reader >> mk_from_meta;
      m_reader >> vt;
      meta_ttl_idx_++;
      c = mk_from_meta.compare(mk_from_data);
    }
    if (0 != c) {
      TRAC("TTL_StreamReader: num = %zd, idx = %zd, cmp(%s, %s) = %d",
           meta_ttl_num_, meta_ttl_idx_, mk_from_meta, mk_from_data, c);
    }
    return 0 == c;
  }
  void OpenFile(const CompactionParams& cp) {
    info_log = cp.info_log;
    // stick to dcompact_worker.cpp
    using std::string;
    static const string NFS_MOUNT_ROOT = GetDirFromEnv("NFS_MOUNT_ROOT", "/mnt/nfs");
    const string& new_prefix = MakePath(NFS_MOUNT_ROOT, cp.instance_name);
    const string& hoster_dir = cp.cf_paths[0].path;
    const string& worker_dir = ReplacePrefix(cp.hoster_root, new_prefix, hoster_dir);
    std::string fpath = worker_dir;
    char buf[32];
    fpath.append(buf, sprintf(buf, "/job-%05d/ttl", cp.job_id));
    //m_file.open(fpath, O_RDONLY, 0777);
    m_file.open("zstd -qdcf " + fpath, "r");
    m_reader.attach(&m_file);
    m_reader.set_bufsize(32*1024);
  }
};

class WorkerBaseDataFilter : public BaseDataFilter {
public:
  WorkerBaseDataFilter() : BaseDataFilter(nullptr, nullptr) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {

    fl_cnt.exec_filter_times++;

    ParsedBaseDataKey parsed_base_data_key(key, &parse_key_buf_);
    Trace("==========================START==========================");
    Trace("[DataFilter], key: %s, data = %s, version = %d",
          parsed_base_data_key.key().ToString().c_str(),
          parsed_base_data_key.data().ToString().c_str(),
          parsed_base_data_key.version());

    Slice new_cur_key = parsed_base_data_key.key();
    if (new_cur_key != cur_key_) {
      cur_key_.assign(new_cur_key.data_, new_cur_key.size_);
      if (m_ttl.ReadUntil(cur_key_)) {
        meta_not_found_ = false;
        cur_meta_version_ = m_ttl.vt.version;
        cur_meta_timestamp_ = m_ttl.vt.timestamp;
      } else {
        meta_not_found_ = true;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      fl_cnt.deleted_not_found.count_info(key, value);
      return true;
    }

    if (cur_meta_timestamp_ != 0
      && cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[Timeout]");
      fl_cnt.deleted_expired.count_info(key, value);
      return true;
    }

    if (cur_meta_version_ > parsed_base_data_key.version()) {
      Trace("Drop[data_key_version < cur_meta_version]");
      fl_cnt.deleted_versions_old.count_info(key, value);
      return true;
    } else {
      Trace("Reserve[data_key_version == cur_meta_version]");
      fl_cnt.all_retained.count_info(key, value);
      return false;
    }
  }
  mutable TTL_StreamReader m_ttl;
};

class WorkerListsDataFilter : public ListsDataFilter {
public:
  WorkerListsDataFilter() : ListsDataFilter(nullptr, nullptr) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {
              
    fl_cnt.exec_filter_times++;

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
      if (m_ttl.ReadUntil(cur_key_)) {
        meta_not_found_ = false;
        cur_meta_version_ = m_ttl.vt.version;
        cur_meta_timestamp_ = m_ttl.vt.timestamp;
      } else {
        meta_not_found_ = true;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      fl_cnt.deleted_not_found.count_info(key, value);
      return true;
    }

    if (cur_meta_timestamp_ != 0
      && cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[Timeout]");
      fl_cnt.deleted_expired.count_info(key, value);
      return true;
    }

    if (cur_meta_version_ > parsed_lists_data_key.version()) {
      Trace("Drop[list_data_key_version < cur_meta_version]");
      fl_cnt.deleted_versions_old.count_info(key, value);
      return true;
    } else {
      Trace("Reserve[list_data_key_version == cur_meta_version]");
      fl_cnt.all_retained.count_info(key, value);
      return false;
    }
  }
  mutable TTL_StreamReader m_ttl;
};

class WorkerZSetsScoreFilter : public ZSetsScoreFilter {
public:
  WorkerZSetsScoreFilter() : ZSetsScoreFilter(nullptr, nullptr) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {

    fl_cnt.exec_filter_times++;

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
      if (m_ttl.ReadUntil(cur_key_)) {
        meta_not_found_ = false;
        cur_meta_version_ = m_ttl.vt.version;
        cur_meta_timestamp_ = m_ttl.vt.timestamp;
      } else {
        meta_not_found_ = true;
      }
    }

    if (meta_not_found_) {
      Trace("Drop[Meta key not exist]");
      fl_cnt.deleted_not_found.count_info(key, value);
      return true;
    }

    if (cur_meta_timestamp_ != 0 &&
        cur_meta_timestamp_ < static_cast<int32_t>(unix_time)) {
      Trace("Drop[Timeout]");
      fl_cnt.deleted_expired.count_info(key, value);
      return true;
    }
    if (cur_meta_version_ > parsed_zsets_score_key.version()) {
      Trace("Drop[score_key_version < cur_meta_version]");
      fl_cnt.deleted_versions_old.count_info(key, value);
      return true;
    } else {
      Trace("Reserve[score_key_version == cur_meta_version]");
      fl_cnt.all_retained.count_info(key, value);
      return false;
    }
  }
  mutable TTL_StreamReader m_ttl;
};

template<class HosterFilter, class Factory>
static std::unique_ptr<CompactionFilter>
HosterSideNewFilter(Factory* fac, const CompactionFilterContext& ctx) {
    DB** dbp = fac->db_ptr_;
    auto filter = new HosterFilter(dbp ? *dbp : NULL, fac->cf_handles_ptr_);
    filter->smallest_seqno_ = ctx.smallest_seqno;
    rocksdb::Env::Default()->GetCurrentTime(&filter->unix_time);
    filter->factory = fac;  // give the Factory pointer to the filer
    return std::unique_ptr<CompactionFilter>(filter);
}

template<class WorkerFilter, class HosterFilter, class Factory>
static std::unique_ptr<CompactionFilter>
SideNewFilter(Factory* fac, const CompactionFilterContext& ctx) {
 if (IsCompactionWorker()) {
    auto filter = new WorkerFilter();
    filter->unix_time = fac->unix_time_;
    filter->m_ttl.OpenFile(*fac->m_cp);
    filter->m_ttl.meta_ttl_num_ = fac->meta_ttl_num_;
    filter->factory = fac;  // give the Factory pointer to the filer
    return std::unique_ptr<CompactionFilter>(filter);
  } else {
    fac->TrySetDBptr();
    return HosterSideNewFilter<HosterFilter>(fac, ctx);
  }
}

#define BW_SideNewFilter(Filter) \
std::unique_ptr<CompactionFilter> \
Filter##Factory::CreateCompactionFilter(const CompactionFilterContext& ctx) { \
  return HosterSideNewFilter<Filter>(this, ctx); \
} \
template<> \
std::unique_ptr<CompactionFilter> \
FilterFac<Filter##Factory>::CreateCompactionFilter(const CompactionFilterContext& ctx) { \
  return SideNewFilter<Worker##Filter, Filter>(this, ctx); \
}

BW_SideNewFilter(BaseDataFilter);
BW_SideNewFilter(ListsDataFilter);
BW_SideNewFilter(ZSetsScoreFilter);

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

inline static const char* pathbasename(Slice p) {
  auto sep = (const char*)memrchr(p.data_, '/', p.size_);
  if (sep)
    return sep + 1;
  else
    return p.data_;
}

Iterator* NewMetaIter(DB* db, ColumnFamilyHandle* cfh,
                      uint64_t smallest_seqno) {
// DEBG("NewMetaIter:%s.%s: smallest_seqno = %zd",
//       pathbasename(db->GetName()), cfh->GetName(), size_t(smallest_seqno));
  ReadOptions rdo;
  if (smallest_seqno > 0) {
    rdo.table_filter = [smallest_seqno]
    (const TableProperties&, const FileMetaData& fmd) {
      // do not retrieve too old data
      return fmd.fd.smallest_seqno >= smallest_seqno;
    };
  }
  return db->NewIterator(rdo, cfh);
}

static
size_t write_ttl_file(const CompactionParams& cp,
                 const std::string& type,
                 const CompactionFilterFactory& fac,
                 DB** dbpp, std::vector<ColumnFamilyHandle*>* cfh_vec,
                 VersionTimestamp (*decode)(std::string*))
{
  Logger* info_log = cp.info_log;
  const std::string start = decode_00_0n(cp.smallest_user_key);
  const std::string bound = decode_00_0n(cp.largest_user_key);
  using namespace std::chrono;
  auto t0 = steady_clock::now();
  size_t bytes = 0, num = 0;
  std::string fpath = cp.cf_paths[0].path;
{
  char buf[32];
  fpath.append(buf, sprintf(buf, "/job-%05d/ttl", cp.job_id));
  //OsFileStream fp(fpath, O_WRONLY|O_CREAT, 0777);
  ProcPipeStream fp("zstd -qf - -o " + fpath, "w");
  LittleEndianDataOutput<OutputBuffer> dio(&fp);
  dio.set_bufsize(32*1024);

  DB* db = *dbpp;
  ColumnFamilyHandle* cfh = (*cfh_vec)[0];
  std::unique_ptr<Iterator> iter(NewMetaIter(db, cfh, cp.smallest_seqno));
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
    dio.write_var_uint64(k.size_);
    dio.ensureWrite(k.data_, k.size_);
    dio << decode(&meta_value);
    bytes += k.size_ + 1; // 1 for key len byte, for simple, ignore len > 255
    num++;
    iter->Next();
  }
}
  bytes += sizeof(VersionTimestamp) * num;
  auto t1 = steady_clock::now();
  double d = duration_cast<microseconds>(t1-t0).count()/1e6;
  struct stat st = {};
  TERARK_VERIFY_S(lstat(fpath.c_str(), &st) == 0, // lstat must success
       "job-%05d: %s.%s lstat(%s) = %m", cp.job_id, type, fac.Name(), fpath);
  INFO("job-%05d: %s.%s.Serialize: tim %8.4f sec, %8.6f Mkv, %9.6f MB, zip %9.6f MB, start = %s, bound = %s",
        cp.job_id, type, fac.Name(), d, num/1e6, bytes/1e6, st.st_size/1e6, start, bound);
  cp.extra_serde_files.push_back("ttl");
  return num;
}

template<class Factory, class ParsedMetaValue>
struct DataFilterFactorySerDe : SerDeFunc<CompactionFilterFactory> {
  using ConcreteFactory = FilterFac<Factory>;
  const CompactionParams* m_cp;
  rocksdb::Logger* info_log;
  int job_id;
  size_t rawzip[2];
  DataFilterFactorySerDe(const json& js, const SidePluginRepo& repo) {
    auto cp = JS_CompactionParamsDecodePtr(js);
    m_cp = cp;
    info_log = cp->info_log;

    // pika requires 1==max_subcompactions, this makes all things simpler
    //
    // 1==max_subcompactions is not required for Dcompact, but it is too
    // complicated to gracefully support multi sub compact in Dcompact,
    // such as this use case, it requires DB side and Worker side has same
    // sub compact boundary to reading keys by streaming.
    TERARK_VERIFY_EQ(cp->max_subcompactions, 1);

    const auto& smallest_user_key = cp->smallest_user_key;
    const auto& largest_user_key = cp->largest_user_key;
    job_id = cp->job_id;
    cp->InputBytes(rawzip);
    TRAC("%s: job_id = %d, smallest_user_key = %s, largest_user_key = %s, job raw = %.3f GB, zip = %.3f GB",
        boost::core::demangle(typeid(DataFilterFactorySerDe).name()).c_str(),
        cp->job_id, smallest_user_key.c_str(), largest_user_key.c_str(), rawzip[0]/1e9, rawzip[1]/1e9);
  }
  void Serialize(FILE* output, const CompactionFilterFactory& base)
  const override {
    auto& fac = dynamic_cast<const ConcreteFactory&>(base);
    LittleEndianDataOutput<NonOwnerFileStream> dio(output);
    if (IsCompactionWorker()) {
      dio << fac.local_fl_cnt;
    }
    else {
      size_t kvs; // meta_ttl_num_
      if (const_cast<ConcreteFactory&>(fac).TrySetDBptr()) {
        kvs = write_ttl_file(*m_cp, fac.m_type, fac,
          fac.db_ptr_, fac.cf_handles_ptr_, &DecodeVT<ParsedMetaValue>);
      }
      else {
        kvs = 0;
        // now it is in DB Open, do not load ttlmap
        DEBG("job-%05d: %s.%s.Serialize: db is in opening",
              job_id, fac.m_type.c_str(), fac.Name());
      }
      int64_t unix_time;
      rocksdb::Env::Default()->GetCurrentTime(&unix_time);
      dio << unix_time;
      dio << kvs;
      DEBG("job-%05d: %s.%s.Serialize: kvs = %zd, job raw = %.3f GB, zip = %.3f GB, smallest_seqno = %lld",
            job_id, fac.m_type.c_str(), fac.Name(), kvs, rawzip[0]/1e9, rawzip[1]/1e9, (llong)m_cp->smallest_seqno);
    }
  }
  void DeSerialize(FILE* reader, CompactionFilterFactory* base)
  const override {
    auto fac = dynamic_cast<ConcreteFactory*>(base);
    LittleEndianDataInput<NonOwnerFileStream> dio(reader);
    if (IsCompactionWorker()) {
      fac->m_cp = this->m_cp;
      dio >> fac->unix_time_;
      dio >> fac->meta_ttl_num_;
      auto kvs = fac->meta_ttl_num_;
      DEBG("job-%05d: %s.%s.DeSerialize: kvs = %zd, job raw = %.3f GB, zip = %.3f GB, smallest_seqno = %lld",
            job_id, fac->m_type.c_str(), fac->Name(), kvs, rawzip[0]/1e9, rawzip[1]/1e9, (llong)m_cp->smallest_seqno);
    }
    else {
      FilterCounter temp_fl_cnt;
      dio >> temp_fl_cnt;
      fac->remote_fl_cnt.add(temp_fl_cnt);
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

void AppendIsoDateTime(std::string* str, time_t t) {
  struct tm   ti;
  struct tm* pti = localtime_r(&t, &ti);
  size_t oldsize = str->size();
  str->resize(oldsize + 64);
  str->resize(oldsize + strftime(&(*str)[oldsize], 64, "%F %T", pti));
}
struct BaseDataKeyDecoder : public UserKeyCoder {
  const char* Name() const override { return "BaseDataKeyDecoder"; }
  void Update(const json&, const SidePluginRepo&) override {
  }
  std::string ToString(const json&, const SidePluginRepo&) const override {
    return "This is the BaseDataKeyDecoder for hashes, sets and zsets.<br/>"
           "The format is <strong>key:<em>version(datetime)</em>:field</strong>";
  }
  void Encode(Slice, std::string*) const override {
    TERARK_DIE("This function should not be called");
  }
  void Decode(Slice coded, std::string* de) const override {
    std::string tmp_s;
    blackwidow::ParsedBaseDataKey tmp_p(coded, &tmp_s);
    auto k = tmp_p.key();
    auto d = tmp_p.data();
    de->clear();
    de->reserve(k.size() + 1 + 32 + 1 + d.size());
    HtmlAppendEscape(de, k.data(), k.size());
    de->append("<em>:");
    AppendIsoDateTime(de, tmp_p.version());
    de->append(":</em>");
    HtmlAppendEscape(de, d.data(), d.size());
  }
};
ROCKSDB_REG_DEFAULT_CONS(BaseDataKeyDecoder, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("BaseDataKeyDecoder");

struct ZSetsScoreKeyDecoder : public UserKeyCoder {
  const char* Name() const override { return "ZSetsScoreKeyDecoder"; }
  void Update(const json&, const SidePluginRepo&) override {
  }
  std::string ToString(const json&, const SidePluginRepo&) const override {
    return "This is the ZSetsScoreKeyDecoder.<br/>"
           "The format is <strong>key:<em>version(datetime)</em>:score:member</strong>";
  }
  void Encode(Slice, std::string*) const override {
    TERARK_DIE("This function should not be called");
  }
  void Decode(Slice coded, std::string* de) const override {
    std::string tmp_s;
    blackwidow::ParsedZSetsScoreKey tmp_p(coded, &tmp_s);
    auto k = tmp_p.key();
    auto s = std::to_string(tmp_p.score());
    auto m = tmp_p.member();
    de->clear();
    de->reserve(k.size() + 1 + 32 + 1 + s.size() + 1 + m.size());
    HtmlAppendEscape(de, k.data(), k.size());
    de->append("<em>:");
    AppendIsoDateTime(de, tmp_p.version());
    de->append("</em>:<em>");
    de->append(s);
    de->append(":</em>");
    HtmlAppendEscape(de, m.data(), m.size());
  }
};
ROCKSDB_REG_DEFAULT_CONS(ZSetsScoreKeyDecoder, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("ZSetsScoreKeyDecoder");

struct ListsDataKeyDecoder : public UserKeyCoder {
  const char* Name() const override { return "ListsDataKeyDecoder"; }
  void Update(const json&, const SidePluginRepo&) override {
  }
  std::string ToString(const json&, const SidePluginRepo&) const override {
    return "This is the ListsDataKeyDecoder.<br/>"
           "The format is <strong>key:<em>version(datetime)</em>:index</strong>";
  }
  void Encode(Slice, std::string*) const override {
    TERARK_DIE("This function should not be called");
  }
  void Decode(Slice coded, std::string* de) const override {
    std::string tmp_s;
    blackwidow::ParsedListsDataKey tmp_p(coded, &tmp_s);
    auto k = tmp_p.key();
    auto i = std::to_string(tmp_p.index());
    de->clear();
    de->reserve(coded.size_ + 32);
    HtmlAppendEscape(de, k.data(), k.size());
    de->append("<em>:");
    AppendIsoDateTime(de, tmp_p.version());
    de->append(":</em>");
    de->append(i);
  }
};
ROCKSDB_REG_DEFAULT_CONS(ListsDataKeyDecoder, AnyPlugin);
ROCKSDB_REG_AnyPluginManip("ListsDataKeyDecoder");

template <class FilterFactory>
struct FilterFactory_Manip : PluginManipFunc<CompactionFilterFactory> {
  void Update(rocksdb::CompactionFilterFactory *, const json &js,
              const SidePluginRepo &repo) const final {}
  std::string ToString(const CompactionFilterFactory &fac,
                       const json &dump_options,
                       const SidePluginRepo &) const final {
    if (auto f = dynamic_cast<const FilterFactory *>(&fac)) {
      FilterCounter &local_fl_cnt = f->local_fl_cnt;
      FilterCounter &remote_fl_cnt = f->remote_fl_cnt;

      bool metric = JsonSmartBool(dump_options, "metric", false);
      if (metric) {
        std::ostringstream oss;
        oss << "local:exec_filter_times " << local_fl_cnt.exec_filter_times << '\n';
        oss << "local:retained:total_num " << local_fl_cnt.all_retained.num << '\n';
        oss << "local:retained:keys_size " << local_fl_cnt.all_retained.keys_size
            << '\n';
        oss << "local:retained:vals_size " << local_fl_cnt.all_retained.vals_size
            << '\n';
        oss << "local:retained:total_size "
            << local_fl_cnt.all_retained.keys_size + local_fl_cnt.all_retained.vals_size
            << '\n';
        oss << "local:deleted:not_found:num " << local_fl_cnt.deleted_not_found.num
            << '\n';
        oss << "local:deleted:not_found:keys_size "
            << local_fl_cnt.deleted_not_found.keys_size << '\n';
        oss << "local:deleted:not_found:vals_size "
            << local_fl_cnt.deleted_not_found.vals_size << '\n';
        oss << "local:deleted:not_found:total_size "
            << local_fl_cnt.deleted_not_found.keys_size +
                   local_fl_cnt.deleted_not_found.vals_size
            << '\n';
        oss << "local:deleted:expired:num " << local_fl_cnt.deleted_expired.num
            << '\n';
        oss << "local:deleted:expired:keys_size "
            << local_fl_cnt.deleted_expired.keys_size << '\n';
        oss << "local:deleted:expired:vals_size "
            << local_fl_cnt.deleted_expired.vals_size << '\n';
        oss << "local:deleted:expired:total_size "
            << local_fl_cnt.deleted_expired.keys_size +
                   local_fl_cnt.deleted_expired.vals_size
            << '\n';
        oss << "local:deleted:versions_old:num "
            << local_fl_cnt.deleted_versions_old.num << '\n';
        oss << "local:deleted:versions_old:keys_size "
            << local_fl_cnt.deleted_versions_old.keys_size << '\n';
        oss << "local:deleted:versions_old:vals_size "
            << local_fl_cnt.deleted_versions_old.vals_size << '\n';
        oss << "local:deleted:versions_old:total_size "
            << local_fl_cnt.deleted_versions_old.keys_size +
                   local_fl_cnt.deleted_versions_old.vals_size
            << '\n';
        oss << "local:deleted:total_num "
            << local_fl_cnt.deleted_not_found.num + local_fl_cnt.deleted_expired.num +
                   local_fl_cnt.deleted_versions_old.num
            << '\n';
        oss << "local:deleted:total_keys_size "
            << local_fl_cnt.deleted_not_found.keys_size +
                   local_fl_cnt.deleted_expired.keys_size +
                   local_fl_cnt.deleted_versions_old.keys_size
            << '\n';
        oss << "local:deleted:total_vals_size "
            << local_fl_cnt.deleted_not_found.vals_size +
                   local_fl_cnt.deleted_expired.vals_size +
                   local_fl_cnt.deleted_versions_old.vals_size
            << '\n';
        oss << "local:deleted:total_size "
            << local_fl_cnt.deleted_not_found.keys_size +
                   local_fl_cnt.deleted_not_found.vals_size +
                   local_fl_cnt.deleted_expired.keys_size +
                   local_fl_cnt.deleted_expired.vals_size +
                   local_fl_cnt.deleted_versions_old.keys_size +
                   local_fl_cnt.deleted_versions_old.vals_size
            << '\n';

        oss << "remote:exec_filter_times " << remote_fl_cnt.exec_filter_times
            << '\n';
        oss << "remote:retained:total_num " << remote_fl_cnt.all_retained.num
            << '\n';
        oss << "remote:retained:keys_size " << remote_fl_cnt.all_retained.keys_size
            << '\n';
        oss << "remote:retained:vals_size " << remote_fl_cnt.all_retained.vals_size
            << '\n';
        oss << "remote:retained:total_size "
            << remote_fl_cnt.all_retained.keys_size +
                   remote_fl_cnt.all_retained.vals_size
            << '\n';
        oss << "remote:deleted:not_found:num "
            << remote_fl_cnt.deleted_not_found.num << '\n';
        oss << "remote:deleted:not_found:keys_size "
            << remote_fl_cnt.deleted_not_found.keys_size << '\n';
        oss << "remote:deleted:not_found:vals_size "
            << remote_fl_cnt.deleted_not_found.vals_size << '\n';
        oss << "remote:deleted:not_found:total_size "
            << remote_fl_cnt.deleted_not_found.keys_size +
                   remote_fl_cnt.deleted_not_found.vals_size
            << '\n';
        oss << "remote:deleted:expired:num " << remote_fl_cnt.deleted_expired.num
            << '\n';
        oss << "remote:deleted:expired:keys_size "
            << remote_fl_cnt.deleted_expired.keys_size << '\n';
        oss << "remote:deleted:expired:vals_size "
            << remote_fl_cnt.deleted_expired.vals_size << '\n';
        oss << "remote:deleted:expired:total_size "
            << remote_fl_cnt.deleted_expired.keys_size +
                   remote_fl_cnt.deleted_expired.vals_size
            << '\n';
        oss << "remote:deleted:versions_old:num "
            << remote_fl_cnt.deleted_versions_old.num << '\n';
        oss << "remote:deleted:versions_old:keys_size "
            << remote_fl_cnt.deleted_versions_old.keys_size << '\n';
        oss << "remote:deleted:versions_old:vals_size "
            << remote_fl_cnt.deleted_versions_old.vals_size << '\n';
        oss << "remote:deleted:versions_old:total_size "
            << remote_fl_cnt.deleted_versions_old.keys_size +
                   remote_fl_cnt.deleted_versions_old.vals_size
            << '\n';
        oss << "remote:deleted:total_num "
            << remote_fl_cnt.deleted_not_found.num + remote_fl_cnt.deleted_expired.num +
                   remote_fl_cnt.deleted_versions_old.num
            << '\n';
        oss << "remote:deleted:total_keys_size "
            << remote_fl_cnt.deleted_not_found.keys_size +
                   remote_fl_cnt.deleted_expired.keys_size +
                   remote_fl_cnt.deleted_versions_old.keys_size
            << '\n';
        oss << "remote:deleted:total_vals_size "
            << remote_fl_cnt.deleted_not_found.vals_size +
                   remote_fl_cnt.deleted_expired.vals_size +
                   remote_fl_cnt.deleted_versions_old.vals_size
            << '\n';
        oss << "remote:deleted:total_size "
            << remote_fl_cnt.deleted_not_found.keys_size +
                   remote_fl_cnt.deleted_not_found.vals_size +
                   remote_fl_cnt.deleted_expired.keys_size +
                   remote_fl_cnt.deleted_expired.vals_size +
                   remote_fl_cnt.deleted_versions_old.keys_size +
                   remote_fl_cnt.deleted_versions_old.vals_size
            << '\n';
        return oss.str();
      }

      json js;

      js["local"]["exec_filter_times"] = local_fl_cnt.exec_filter_times;
      js["local"]["retained"]["total_num"] = local_fl_cnt.all_retained.num;
      js["local"]["deleted"]["not_found"]["num"] = local_fl_cnt.deleted_not_found.num;
      js["local"]["deleted"]["expired"]["num"] = local_fl_cnt.deleted_expired.num;
      js["local"]["deleted"]["versions_old"]["num"] =
          local_fl_cnt.deleted_versions_old.num;
      js["local"]["deleted"]["total_num"] = local_fl_cnt.deleted_not_found.num +
                                            local_fl_cnt.deleted_expired.num +
                                            local_fl_cnt.deleted_versions_old.num;

      js["remote"]["exec_filter_times"] = remote_fl_cnt.exec_filter_times;
      js["remote"]["retained"]["total_num"] = remote_fl_cnt.all_retained.num;
      js["remote"]["deleted"]["not_found"]["num"] = remote_fl_cnt.deleted_not_found.num;
      js["remote"]["deleted"]["expired"]["num"] = remote_fl_cnt.deleted_expired.num;
      js["remote"]["deleted"]["versions_old"]["num"] =
          remote_fl_cnt.deleted_versions_old.num;
      js["remote"]["deleted"]["total_num"] = remote_fl_cnt.deleted_not_found.num +
                                            remote_fl_cnt.deleted_expired.num +
                                            remote_fl_cnt.deleted_versions_old.num;

      if (JsonSmartBool(dump_options, "html", true)) {
        js["local"]["retained"]["keys_size"] =
            SizeToString(local_fl_cnt.all_retained.keys_size);
        js["local"]["retained"]["vals_size"] =
            SizeToString(local_fl_cnt.all_retained.vals_size);
        js["local"]["retained"]["total_size"] =
            SizeToString(local_fl_cnt.all_retained.keys_size + local_fl_cnt.all_retained.vals_size);
        js["local"]["deleted"]["not_found"]["keys_size"] =
            SizeToString(local_fl_cnt.deleted_not_found.keys_size);
        js["local"]["deleted"]["not_found"]["vals_size"] =
            SizeToString(local_fl_cnt.deleted_not_found.vals_size);
        js["local"]["deleted"]["not_found"]["total_size"] = SizeToString(
            local_fl_cnt.deleted_not_found.keys_size + local_fl_cnt.deleted_not_found.vals_size);
        js["local"]["deleted"]["expired"]["keys_size"] =
            SizeToString(local_fl_cnt.deleted_expired.keys_size);
        js["local"]["deleted"]["expired"]["vals_size"] =
            SizeToString(local_fl_cnt.deleted_expired.vals_size);
        js["local"]["deleted"]["expired"]["total_size"] = SizeToString(
            local_fl_cnt.deleted_expired.keys_size + local_fl_cnt.deleted_expired.vals_size);
        js["local"]["deleted"]["versions_old"]["keys_size"] =
            SizeToString(local_fl_cnt.deleted_versions_old.keys_size);
        js["local"]["deleted"]["versions_old"]["vals_size"] =
            SizeToString(local_fl_cnt.deleted_versions_old.vals_size);
        js["local"]["deleted"]["versions_old"]["total_size"] =
            SizeToString(local_fl_cnt.deleted_versions_old.keys_size +
                         local_fl_cnt.deleted_versions_old.vals_size);
        js["local"]["deleted"]["total_keys_size"] = SizeToString(
            local_fl_cnt.deleted_not_found.keys_size + local_fl_cnt.deleted_expired.keys_size +
            local_fl_cnt.deleted_versions_old.keys_size);
        js["local"]["deleted"]["total_vals_size"] = SizeToString(
            local_fl_cnt.deleted_not_found.vals_size + local_fl_cnt.deleted_expired.vals_size +
            local_fl_cnt.deleted_versions_old.vals_size);
        js["local"]["deleted"]["total_size"] = SizeToString(
            local_fl_cnt.deleted_not_found.keys_size + local_fl_cnt.deleted_not_found.vals_size +
            local_fl_cnt.deleted_expired.keys_size + local_fl_cnt.deleted_expired.vals_size +
            local_fl_cnt.deleted_versions_old.keys_size +
            local_fl_cnt.deleted_versions_old.vals_size);

        js["remote"]["retained"]["keys_size"] =
            SizeToString(remote_fl_cnt.all_retained.keys_size);
        js["remote"]["retained"]["vals_size"] =
            SizeToString(remote_fl_cnt.all_retained.vals_size);
        js["remote"]["retained"]["total_size"] =
            SizeToString(remote_fl_cnt.all_retained.keys_size + remote_fl_cnt.all_retained.vals_size);
        js["remote"]["deleted"]["not_found"]["keys_size"] =
            SizeToString(remote_fl_cnt.deleted_not_found.keys_size);
        js["remote"]["deleted"]["not_found"]["vals_size"] =
            SizeToString(remote_fl_cnt.deleted_not_found.vals_size);
        js["remote"]["deleted"]["not_found"]["total_size"] = SizeToString(
            remote_fl_cnt.deleted_not_found.keys_size + remote_fl_cnt.deleted_not_found.vals_size);
        js["remote"]["deleted"]["expired"]["keys_size"] =
            SizeToString(remote_fl_cnt.deleted_expired.keys_size);
        js["remote"]["deleted"]["expired"]["vals_size"] =
            SizeToString(remote_fl_cnt.deleted_expired.vals_size);
        js["remote"]["deleted"]["expired"]["total_size"] = SizeToString(
            remote_fl_cnt.deleted_expired.keys_size + remote_fl_cnt.deleted_expired.vals_size);
        js["remote"]["deleted"]["versions_old"]["keys_size"] =
            SizeToString(remote_fl_cnt.deleted_versions_old.keys_size);
        js["remote"]["deleted"]["versions_old"]["vals_size"] =
            SizeToString(remote_fl_cnt.deleted_versions_old.vals_size);
        js["remote"]["deleted"]["versions_old"]["total_size"] =
            SizeToString(remote_fl_cnt.deleted_versions_old.keys_size +
                         remote_fl_cnt.deleted_versions_old.vals_size);
        js["remote"]["deleted"]["total_keys_size"] = SizeToString(
            remote_fl_cnt.deleted_not_found.keys_size + remote_fl_cnt.deleted_expired.keys_size +
            remote_fl_cnt.deleted_versions_old.keys_size);
        js["remote"]["deleted"]["total_vals_size"] = SizeToString(
            remote_fl_cnt.deleted_not_found.vals_size + remote_fl_cnt.deleted_expired.vals_size +
            remote_fl_cnt.deleted_versions_old.vals_size);
        js["remote"]["deleted"]["total_size"] = SizeToString(
            remote_fl_cnt.deleted_not_found.keys_size + remote_fl_cnt.deleted_not_found.vals_size +
            remote_fl_cnt.deleted_expired.keys_size + remote_fl_cnt.deleted_expired.vals_size +
            remote_fl_cnt.deleted_versions_old.keys_size +
            remote_fl_cnt.deleted_versions_old.vals_size);
      } else {
        js["local"]["retained"]["keys_size"] = local_fl_cnt.all_retained.keys_size;
        js["local"]["retained"]["vals_size"] = local_fl_cnt.all_retained.vals_size;
        js["local"]["retained"]["total_size"] =
            local_fl_cnt.all_retained.keys_size + local_fl_cnt.all_retained.vals_size;
        js["local"]["deleted"]["not_found"]["keys_size"] =
            local_fl_cnt.deleted_not_found.keys_size;
        js["local"]["deleted"]["not_found"]["vals_size"] =
            local_fl_cnt.deleted_not_found.vals_size;
        js["local"]["deleted"]["not_found"]["total_size"] =
            local_fl_cnt.deleted_not_found.keys_size + local_fl_cnt.deleted_not_found.vals_size;
        js["local"]["deleted"]["expired"]["keys_size"] =
            local_fl_cnt.deleted_expired.keys_size;
        js["local"]["deleted"]["expired"]["vals_size"] =
            local_fl_cnt.deleted_expired.vals_size;
        js["local"]["deleted"]["expired"]["total_size"] =
            local_fl_cnt.deleted_expired.keys_size + local_fl_cnt.deleted_expired.vals_size;
        js["local"]["deleted"]["versions_old"]["keys_size"] =
            local_fl_cnt.deleted_versions_old.keys_size;
        js["local"]["deleted"]["versions_old"]["vals_size"] =
            local_fl_cnt.deleted_versions_old.vals_size;
        js["local"]["deleted"]["versions_old"]["total_size"] =
            local_fl_cnt.deleted_versions_old.keys_size +
            local_fl_cnt.deleted_versions_old.vals_size;
        js["local"]["deleted"]["total_keys_size"] =
            local_fl_cnt.deleted_not_found.keys_size + local_fl_cnt.deleted_expired.keys_size +
            local_fl_cnt.deleted_versions_old.keys_size;
        js["local"]["deleted"]["total_vals_size"] =
            local_fl_cnt.deleted_not_found.vals_size + local_fl_cnt.deleted_expired.vals_size +
            local_fl_cnt.deleted_versions_old.vals_size;
        js["local"]["deleted"]["total_size"] =
            local_fl_cnt.deleted_not_found.keys_size + local_fl_cnt.deleted_not_found.vals_size +
            local_fl_cnt.deleted_expired.keys_size + local_fl_cnt.deleted_expired.vals_size +
            local_fl_cnt.deleted_versions_old.keys_size +
            local_fl_cnt.deleted_versions_old.vals_size;

        js["remote"]["retained"]["keys_size"] = remote_fl_cnt.all_retained.keys_size;
        js["remote"]["retained"]["vals_size"] = remote_fl_cnt.all_retained.vals_size;
        js["remote"]["retained"]["total_size"] =
            remote_fl_cnt.all_retained.keys_size + remote_fl_cnt.all_retained.vals_size;
        js["remote"]["deleted"]["not_found"]["keys_size"] =
            remote_fl_cnt.deleted_not_found.keys_size;
        js["remote"]["deleted"]["not_found"]["vals_size"] =
            remote_fl_cnt.deleted_not_found.vals_size;
        js["remote"]["deleted"]["not_found"]["total_size"] =
            remote_fl_cnt.deleted_not_found.keys_size + remote_fl_cnt.deleted_not_found.vals_size;
        js["remote"]["deleted"]["expired"]["keys_size"] =
            remote_fl_cnt.deleted_expired.keys_size;
        js["remote"]["deleted"]["expired"]["vals_size"] =
            remote_fl_cnt.deleted_expired.vals_size;
        js["remote"]["deleted"]["expired"]["total_size"] =
            remote_fl_cnt.deleted_expired.keys_size + remote_fl_cnt.deleted_expired.vals_size;
        js["remote"]["deleted"]["versions_old"]["keys_size"] =
            remote_fl_cnt.deleted_versions_old.keys_size;
        js["remote"]["deleted"]["versions_old"]["vals_size"] =
            remote_fl_cnt.deleted_versions_old.vals_size;
        js["remote"]["deleted"]["versions_old"]["total_size"] =
            remote_fl_cnt.deleted_versions_old.keys_size +
            remote_fl_cnt.deleted_versions_old.vals_size;
        js["remote"]["deleted"]["total_keys_size"] =
            remote_fl_cnt.deleted_not_found.keys_size + remote_fl_cnt.deleted_expired.keys_size +
            remote_fl_cnt.deleted_versions_old.keys_size;
        js["remote"]["deleted"]["total_vals_size"] =
            remote_fl_cnt.deleted_not_found.vals_size + remote_fl_cnt.deleted_expired.vals_size +
            remote_fl_cnt.deleted_versions_old.vals_size;
        js["remote"]["deleted"]["total_size"] =
            remote_fl_cnt.deleted_not_found.keys_size + remote_fl_cnt.deleted_not_found.vals_size +
            remote_fl_cnt.deleted_expired.keys_size + remote_fl_cnt.deleted_expired.vals_size +
            remote_fl_cnt.deleted_versions_old.keys_size +
            remote_fl_cnt.deleted_versions_old.vals_size;
      }

      return JsonToString(js, dump_options);
    }
    THROW_InvalidArgument("Unknow CompactionFilterFactory");
  }
};

typedef FilterFactory_Manip<BaseMetaFilterFactory> BaseMetaFilterFactory_Manip;
typedef FilterFactory_Manip<BaseDataFilterFactory> BaseDataFilterFactory_Manip;
typedef FilterFactory_Manip<ListsMetaFilterFactory> ListsMetaFilterFactory_Manip;
typedef FilterFactory_Manip<ListsDataFilterFactory> ListsDataFilterFactory_Manip;
typedef FilterFactory_Manip<ZSetsScoreFilterFactory> ZSetsScoreFilterFactory_Manip;
typedef FilterFactory_Manip<StringsFilterFactory> StringsFilterFactory_Manip;

ROCKSDB_REG_PluginManip("BaseMetaFilterFactory", BaseMetaFilterFactory_Manip);
ROCKSDB_REG_PluginManip("BaseDataFilterFactory", BaseDataFilterFactory_Manip);
ROCKSDB_REG_PluginManip("ListsMetaFilterFactory", ListsMetaFilterFactory_Manip);
ROCKSDB_REG_PluginManip("ListsDataFilterFactory", ListsDataFilterFactory_Manip);
ROCKSDB_REG_PluginManip("ZSetsScoreFilterFactory", ZSetsScoreFilterFactory_Manip);
ROCKSDB_REG_PluginManip("StringsFilterFactory", StringsFilterFactory_Manip);


} // namespace blackwidow
