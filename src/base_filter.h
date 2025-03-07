//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_FILTER_H_
#define SRC_BASE_FILTER_H_

#include <string>
#include <memory>
#include <vector>

#include "src/debug.h"
#include "src/base_meta_value_format.h"
#include "src/base_data_key_format.h"
#include "rocksdb/compaction_filter.h"

#include "src/filter_counter.h"

namespace rocksdb {
  class ColumnFamilyHandle;
}
using rocksdb::ColumnFamilyHandle;

namespace blackwidow {

rocksdb::Iterator* NewMetaIter(rocksdb::DB*, ColumnFamilyHandle*, uint64_t);

class BaseMetaFilterFactory;

class BaseMetaFilterFactory;

class BaseMetaFilter : public rocksdb::CompactionFilter {
 public:
  BaseMetaFilter() = default;
  ~BaseMetaFilter();
  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {

    fl_cnt.exec_filter_times++;

    int32_t cur_time = static_cast<int32_t>(unix_time);
    ParsedBaseMetaValue parsed_base_meta_value(value);
    Trace("==========================START==========================");
    Trace("[MetaFilter], key: %s, count = %d, timestamp: %d, cur_time: %d, version: %d",
          key.ToString().c_str(),
          parsed_base_meta_value.count(),
          parsed_base_meta_value.timestamp(),
          cur_time,
          parsed_base_meta_value.version());

    if (parsed_base_meta_value.timestamp() != 0
      && parsed_base_meta_value.timestamp() < cur_time
      && parsed_base_meta_value.version() < cur_time) {
      Trace("Drop[Stale & version < cur_time]");
      fl_cnt.deleted_expired.count_info(key, value);
      return true;
    }
    if (parsed_base_meta_value.count() == 0
      && parsed_base_meta_value.version() < cur_time) {
      Trace("Drop[Empty & version < cur_time]");
      fl_cnt.deleted_versions_old.count_info(key, value);
      return true;
    }
    Trace("Reserve");
    fl_cnt.all_retained.count_info(key, value);
    return false;
  }
  int64_t unix_time = 0;

  mutable FilterCounter fl_cnt;
  const BaseMetaFilterFactory* factory = nullptr;

  const char* Name() const override { return "BaseMetaFilter"; }
};

class BaseMetaFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseMetaFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
        const rocksdb::CompactionFilter::Context& context) override;
  const char* Name() const override {
    return "BaseMetaFilterFactory";
  }
  uint64_t unix_time_ = 0; // only used by compact worker

  mutable FilterCounter local_fl_cnt;
  mutable FilterCounter remote_fl_cnt;
};


class BaseDataFilterFactory;

class BaseDataFilter : public rocksdb::CompactionFilter {
 public:
  ~BaseDataFilter();
  BaseDataFilter(rocksdb::DB* db,
                 std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr) :
    db_(db), iter_(nullptr),
    cf_handles_ptr_(cf_handles_ptr),
    cur_key_(""),
    meta_not_found_(false),
    cur_meta_version_(0),
    cur_meta_timestamp_(0) {}

  bool Filter(int level, const Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {

    fl_cnt.exec_filter_times++;

    if (nullptr == db_ || nullptr == cf_handles_ptr_) {
      return false;
    }
    ParsedBaseDataKey parsed_base_data_key(key, &parse_key_buf_);
    Trace("==========================START==========================");
    Trace("[DataFilter], key: %s, data = %s, version = %d",
          parsed_base_data_key.key().ToString().c_str(),
          parsed_base_data_key.data().ToString().c_str(),
          parsed_base_data_key.version());

    if (parsed_base_data_key.key().ToString() != cur_key_) {
      cur_key_ = parsed_base_data_key.key().ToString();
      std::string meta_value;
      // destroyed when close the database, Reserve Current key value
      if (cf_handles_ptr_->size() == 0) {
        fl_cnt.all_retained.count_info(key, value);
        return false;
      }
    #if 0
      std::string meta_key = decode_01_00(key);
      if (!iter_) {
        iter_ = NewMetaIter(db_, (*cf_handles_ptr_)[0], smallest_seqno_);
        ROCKSDB_VERIFY(nullptr != iter_);
        iter_->Seek(meta_key);
      }
      auto iter = iter_;
      while (iter->Valid() && iter->key() < meta_key) {
        iter->Next();
      }
      Status s;
      if (!iter->Valid()) {
        s = Status::NotFound("iter_->Valid() is false");
      }
      else if (iter->key() != meta_key) {
        s = Status::NotFound("iter_->key() != meta_key");
      }
      else {
        meta_value = iter->value().ToString();
      }
    #else
      Status s = db_->Get(default_read_options_,
              (*cf_handles_ptr_)[0], cur_key_, &meta_value);
    #endif
      if (s.ok()) {
        meta_not_found_ = false;
        ParsedBaseMetaValue parsed_base_meta_value(&meta_value);
        cur_meta_version_ = parsed_base_meta_value.version();
        cur_meta_timestamp_ = parsed_base_meta_value.timestamp();
      } else if (s.IsNotFound()) {
        meta_not_found_ = true;
      } else {
        cur_key_ = "";
        Trace("Reserve[Get meta_key faild]");
        fl_cnt.all_retained.count_info(key, value);
        return false;
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
  int64_t unix_time;

  mutable FilterCounter fl_cnt;
  const BaseDataFilterFactory* factory = nullptr;

  const char* Name() const override { return "BaseDataFilter"; }

  rocksdb::DB* db_;
  mutable rocksdb::Iterator* iter_;
  mutable uint64_t smallest_seqno_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
  rocksdb::ReadOptions default_read_options_;
  mutable std::string parse_key_buf_;
  mutable std::string cur_key_;
  mutable bool meta_not_found_;
  mutable int32_t cur_meta_version_;
  mutable int32_t cur_meta_timestamp_;
};

class BaseDataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  BaseDataFilterFactory() : BaseDataFilterFactory(nullptr, nullptr) {}
  BaseDataFilterFactory(rocksdb::DB** db_ptr,
                        std::vector<rocksdb::ColumnFamilyHandle*>* handles_ptr)
      : db_ptr_(db_ptr), cf_handles_ptr_(handles_ptr) {
  }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context&) override;
  const char* Name() const override {
    return "BaseDataFilterFactory";
  }

  rocksdb::DB** db_ptr_;
  std::vector<rocksdb::ColumnFamilyHandle*>* cf_handles_ptr_;
  uint64_t unix_time_;
  size_t meta_ttl_num_;

  mutable FilterCounter local_fl_cnt;
  mutable FilterCounter remote_fl_cnt;
};

typedef BaseMetaFilter HashesMetaFilter;
typedef BaseMetaFilterFactory HashesMetaFilterFactory;
typedef BaseDataFilter HashesDataFilter;
typedef BaseDataFilterFactory HashesDataFilterFactory;

typedef BaseMetaFilter SetsMetaFilter;
typedef BaseMetaFilterFactory SetsMetaFilterFactory;
typedef BaseDataFilter SetsMemberFilter;
typedef BaseDataFilterFactory SetsMemberFilterFactory;

typedef BaseMetaFilter ZSetsMetaFilter;
typedef BaseMetaFilterFactory ZSetsMetaFilterFactory;
typedef BaseDataFilter ZSetsDataFilter;
typedef BaseDataFilterFactory ZSetsDataFilterFactory;

}  //  namespace blackwidow
#endif  // SRC_BASE_FILTER_H_
