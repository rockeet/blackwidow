//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis_hashes.h"

#include <memory>

#include "blackwidow/util.h"
#include "src/base_filter.h"
#include "src/scope_record_lock.h"
#include "src/scope_snapshot.h"
#include "include/pika_data_length_histogram.h"
#include "blackwidow/slice_hash.h"
#include <terark/valvec.hpp>
#include <terark/util/autofree.hpp>

using namespace terark;
extern length_histogram::CmdDataLengthHistogram* g_pika_cmd_data_length_histogram;

static void HashFieldAddHistogram(size_t field_size, size_t value_size) {
  g_pika_cmd_data_length_histogram->AddLengthMetric(length_histogram::RedisHash, length_histogram::Add, length_histogram::Field, field_size);
  g_pika_cmd_data_length_histogram->AddLengthMetric(length_histogram::RedisHash, length_histogram::Add, length_histogram::Value, value_size);
};
static void HashFieldDelHistogram(size_t field_size, size_t value_size) {
  g_pika_cmd_data_length_histogram->AddLengthMetric(length_histogram::RedisHash, length_histogram::Del, length_histogram::Field, field_size);
  g_pika_cmd_data_length_histogram->AddLengthMetric(length_histogram::RedisHash, length_histogram::Del, length_histogram::Value, value_size);
};
static void HashKeyHistogram(length_histogram::ProcessType add_del, size_t size) {
  g_pika_cmd_data_length_histogram->AddLengthMetric(length_histogram::RedisHash, add_del, length_histogram::Key, size);
}
static auto data_suffix_length = blackwidow::ParsedBaseMetaValue::kBaseMetaValueSuffixLength;

namespace blackwidow {

RedisHashes::RedisHashes(BlackWidow* const bw, const DataType& type)
    : Redis(bw, type) {
}

Status RedisHashes::Open(const BlackwidowOptions& bw_options,
                         const std::string& db_path) {
  statistics_store_->SetCapacity(bw_options.statistics_max_size);
  small_compaction_threshold_ = bw_options.small_compaction_threshold;
  if (!bw_options.sideplugin_conf.empty()) {
    return OpenByRepo(bw_options, db_path, "hashes");
  }

  rocksdb::Options ops(bw_options.options);
  Status s = rocksdb::DB::Open(ops, db_path, &db_);
  if (s.ok()) {
    // create column family
    rocksdb::ColumnFamilyHandle* cf;
    s = db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
        "data_cf", &cf);
    if (!s.ok()) {
      return s;
    }
    // close DB
    delete cf;
    delete db_;
  }

  // Open
  rocksdb::DBOptions db_ops(bw_options.options);
  rocksdb::ColumnFamilyOptions meta_cf_ops(bw_options.options);
  rocksdb::ColumnFamilyOptions data_cf_ops(bw_options.options);
  meta_cf_ops.compaction_filter_factory =
    std::make_shared<HashesMetaFilterFactory>();
  data_cf_ops.compaction_filter_factory =
    std::make_shared<HashesDataFilterFactory>(&db_, &handles_);

  // use the bloom filter policy to reduce disk reads
  rocksdb::BlockBasedTableOptions table_ops(bw_options.table_options);
  table_ops.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, true));
  rocksdb::BlockBasedTableOptions meta_cf_table_ops(table_ops);
  rocksdb::BlockBasedTableOptions data_cf_table_ops(table_ops);
  if (!bw_options.share_block_cache && bw_options.block_cache_size > 0) {
    meta_cf_table_ops.block_cache =
      rocksdb::NewLRUCache(bw_options.block_cache_size);
    data_cf_table_ops.block_cache =
      rocksdb::NewLRUCache(bw_options.block_cache_size);
  }
  meta_cf_ops.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(meta_cf_table_ops));
  data_cf_ops.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(data_cf_table_ops));

  std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
  // Meta CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      rocksdb::kDefaultColumnFamilyName, meta_cf_ops));
  // Data CF
  column_families.push_back(rocksdb::ColumnFamilyDescriptor(
      "data_cf", data_cf_ops));
  return rocksdb::DB::Open(db_ops, db_path, column_families, &handles_, &db_);
}

Status RedisHashes::CompactRange(const rocksdb::Slice* begin,
                                 const rocksdb::Slice* end,
                                 const ColumnFamilyType& type) {
  if (type == kMeta || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[0], begin, end);
  }
  if (type == kData || type == kMetaAndData) {
    db_->CompactRange(default_compact_range_options_, handles_[1], begin, end);
  }
  return Status::OK();
}

Status RedisHashes::GetProperty(const std::string& property, uint64_t* out) {
  std::string value;
  db_->GetProperty(handles_[0], property, &value);
  *out = std::strtoull(value.c_str(), NULL, 10);
  db_->GetProperty(handles_[1], property, &value);
  *out += std::strtoull(value.c_str(), NULL, 10);
  return Status::OK();
}

Status RedisHashes::ScanKeyNum(KeyInfo* key_info) {
  uint64_t keys = 0;
  uint64_t expires = 0;
  uint64_t ttl_sum = 0;
  uint64_t invaild_keys = 0;

  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  int64_t curtime;
  rocksdb::Env::Default()->GetCurrentTime(&curtime);

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      invaild_keys++;
    } else {
      keys++;
      if (!parsed_hashes_meta_value.IsPermanentSurvival()) {
        expires++;
        ttl_sum += parsed_hashes_meta_value.timestamp() - curtime;
      }
    }
  }
  delete iter;

  key_info->keys = keys;
  key_info->expires = expires;
  key_info->avg_ttl = (expires != 0) ? ttl_sum / expires : 0;
  key_info->invaild_keys = invaild_keys;
  return Status::OK();
}

Status RedisHashes::ScanKeys(const std::string& pattern,
                             std::vector<std::string>* keys) {
  std::string key;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  for (iter->SeekToFirst();
       iter->Valid();
       iter->Next()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(iter->value());
    if (!parsed_hashes_meta_value.IsStale()
      && parsed_hashes_meta_value.count() != 0) {
      key = iter->key().ToString();
      if (StringMatch(pattern.data(),
            pattern.size(), key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
    }
  }
  delete iter;
  return Status::OK();
}

Status RedisHashes::PKPatternMatchDel(const std::string& pattern,
                                      int32_t* ret) {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  std::string key;
  std::string meta_value;
  int32_t total_delete = 0;
  Status s;
  rocksdb::WriteBatch batch;
  rocksdb::Iterator* iter = db_->NewIterator(iterator_options, handles_[0]);
  iter->SeekToFirst();
  while (iter->Valid()) {
    key = iter->key().ToString();
    meta_value = iter->value().ToString();
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (!parsed_hashes_meta_value.IsStale()
      && parsed_hashes_meta_value.count()
      && StringMatch(pattern.data(), pattern.size(), key.data(), key.size(), 0)) {
      parsed_hashes_meta_value.InitialMetaValue();
      batch.Put(handles_[0], key, meta_value);
      HashKeyHistogram(length_histogram::Del, key.size());
      HashFieldDelHistogram(key.size(), parsed_hashes_meta_value.user_value().size());
    }
    if (static_cast<size_t>(batch.Count()) >= BATCH_DELETE_LIMIT) {
      s = db_->Write(default_write_options_, &batch);
      if (s.ok()) {
        total_delete += batch.Count();
        batch.Clear();
      } else {
        *ret = total_delete;
        return s;
      }
    }
    iter->Next();
  }
  if (batch.Count()) {
    s = db_->Write(default_write_options_, &batch);
    if (s.ok()) {
      total_delete += batch.Count();
      batch.Clear();
    }
  }

  *ret = total_delete;
  return s;
}

Status RedisHashes::HDel(const Slice& key,
                         const std::vector<std::string>& fields,
                         int32_t* ret) {
  uint32_t statistic = 0;
  std::vector<std::string> filtered_fields;
  std::unordered_set<std::string> field_set;
  for (auto iter = fields.begin(); iter != fields.end(); ++iter) {
    const std::string& field = *iter;
    if (field_set.insert(field).second) {
      filtered_fields.push_back(field);
    }
  }

  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t del_cnt = 0;
  int32_t version = 0;
  ScopeRecordLock l(lock_mgr_, key);
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      *ret = 0;
      return Status::OK();
    } else {
      std::string data_value;
      version = parsed_hashes_meta_value.version();
      for (const auto& field : filtered_fields) {
        HashesDataKey hashes_data_key(key, version, field);
        s = db_->Get(read_options, handles_[1],
                hashes_data_key.Encode(), &data_value);
        if (s.ok()) {
          del_cnt++;
          statistic++;
          batch.Delete(handles_[1], hashes_data_key.Encode());
          HashFieldDelHistogram(field.size(), data_value.size() - data_suffix_length);
        } else if (s.IsNotFound()) {
          continue;
        } else {
          return s;
        }
      }
      *ret = del_cnt;
      parsed_hashes_meta_value.ModifyCount(-del_cnt);
      if (parsed_hashes_meta_value.count() == 0) HashKeyHistogram(length_histogram::Del, key.size());
      batch.Put(handles_[0], key, meta_value);
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
    return Status::OK();
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisHashes::HExists(const Slice& key, const Slice& field) {
  std::string value;
  return HGet(key, field, &value);
}

Status RedisHashes::HGet(const Slice& key, const Slice& field,
                         std::string* value) {
  std::string meta_value;
  int32_t version = 0;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey data_key(key, version, field);
      s = db_->Get(read_options, handles_[1], data_key.Encode(), value);
    }
  }
  return s;
}

Status RedisHashes::HGetall(const Slice& key,
                            std::vector<FieldValue>* fvs) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      std::string parse_key_buf;
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        fvs->push_back({parsed_hashes_data_key.field().ToString(),
                iter->value().ToString()});
      }
      delete iter;
    }
  }
  return s;
}

Status RedisHashes::HIncrby(const Slice& key, const Slice& field, int64_t value,
                            int64_t* ret) {
  *ret = 0;
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  uint32_t statistic = 0;
  std::string old_value;
  std::string meta_value;

  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      char buf[32];
      Int64ToStr(buf, 32, value);
      batch.Put(handles_[1], hashes_data_key.Encode(), buf);
      *ret = value;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[1],
              hashes_data_key.Encode(), &old_value);
      if (s.ok()) {
        int64_t ival = 0;
        if (!StrToInt64(old_value.data(), old_value.size(), &ival)) {
          return Status::Corruption("hash value is not an integer");
        }
        if ((value >= 0 && LLONG_MAX - value < ival) ||
          (value < 0 && LLONG_MIN - value > ival)) {
          return Status::InvalidArgument("Overflow");
        }
        *ret = ival + value;
        char buf[32];
        Int64ToStr(buf, 32, *ret);
        batch.Put(handles_[1], hashes_data_key.Encode(), buf);
        statistic++;
      } else if (s.IsNotFound()) {
        char buf[32];
        Int64ToStr(buf, 32, value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), buf);
        *ret = value;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);

    char buf[32];
    Int64ToStr(buf, 32, value);
    batch.Put(handles_[1], hashes_data_key.Encode(), buf);
    *ret = value;
    HashKeyHistogram(length_histogram::Add, key.size());
    HashFieldAddHistogram(field.size(), std::to_string(value).size());
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisHashes::HIncrbyfloat(const Slice& key, const Slice& field,
                                 const Slice& by, std::string* new_value) {
  new_value->clear();
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  std::string old_value_str;
  long double long_double_by;

  if (StrToLongDouble(by.data(), by.size(), &long_double_by) == -1) {
    return Status::Corruption("value is not a vaild float");
  }

  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.UpdateVersion();
      parsed_hashes_meta_value.set_count(1);
      parsed_hashes_meta_value.set_timestamp(0);
      batch.Put(handles_[0], key, meta_value);
      HashesDataKey hashes_data_key(key, version, field);

      LongDoubleToStr(long_double_by, new_value);
      batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_, handles_[1],
              hashes_data_key.Encode(), &old_value_str);
      if (s.ok()) {
        long double total;
        long double old_value;
        if (StrToLongDouble(old_value_str.data(),
                    old_value_str.size(), &old_value) == -1) {
          return Status::Corruption("value is not a vaild float");
        }

        total = old_value + long_double_by;
        if (LongDoubleToStr(total, new_value) == -1) {
          return Status::InvalidArgument("Overflow");
        }
        batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
        statistic++;
      } else if (s.IsNotFound()) {
        LongDoubleToStr(long_double_by, new_value);
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());

    HashesDataKey hashes_data_key(key, version, field);
    LongDoubleToStr(long_double_by, new_value);
    batch.Put(handles_[1], hashes_data_key.Encode(), *new_value);
    HashKeyHistogram(length_histogram::Add, key.size());
    HashFieldAddHistogram(field.size(), new_value->size());
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisHashes::HKeys(const Slice& key,
                          std::vector<std::string>* fields) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      std::string parse_key_buf;
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        fields->push_back(parsed_hashes_data_key.field().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status RedisHashes::HLen(const Slice& key, int32_t* ret) {
  *ret = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *ret = 0;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      *ret = parsed_hashes_meta_value.count();
    }
  } else if (s.IsNotFound()) {
    *ret = 0;
  }
  return s;
}

Status RedisHashes::HMGet(const Slice& key,
                          const std::vector<std::string>& fields,
                          std::vector<ValueStatus>* vss) {
  vss->clear();

  const size_t num = fields.size();
  int32_t version = 0;
  bool is_stale = false;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    is_stale = parsed_hashes_meta_value.IsStale();
    if (is_stale || parsed_hashes_meta_value.count() == 0) {
      for (size_t idx = 0; idx < num; ++idx) {
        vss->push_back({std::string(), Status::NotFound()});
      }
      return Status::NotFound(is_stale ? "Stale" : "");
    } else {
      version = parsed_hashes_meta_value.version();
      valvec<rocksdb::PinnableSlice> values_data(num);
      valvec<Status>         status_vec(num);
      valvec<HashesDataKey>  fields_key_data(num, valvec_reserve());
      AutoFree<Slice>        fields_key_ref(num);
      for (size_t i = 0; i < num; ++i) {
        fields_key_data.unchecked_emplace_back(key, version, fields[i]);
        fields_key_ref.p[i] = fields_key_data[i].Encode();
      }
      db_->MultiGet(read_options, handles_[1], num,
            fields_key_ref.p, values_data.data(), status_vec.data());
      for (size_t i = 0; i < num; ++i) {
        s = std::move(status_vec[i]);
        if (s.ok()) {
          auto& value = values_data[i];
          if (value.IsPinned())
            vss->push_back({value.ToString(), Status::OK()});
          else
            vss->push_back({std::move(*value.GetSelf()), Status::OK()});
        } else if (s.IsNotFound()) {
          vss->push_back({std::string(), Status::NotFound()});
        } else {
          vss->clear();
          return s;
        }
      }
    }
    return Status::OK();
  } else if (s.IsNotFound()) {
    for (size_t idx = 0; idx < num; ++idx) {
      vss->push_back({std::string(), Status::NotFound()});
    }
  }
  return s;
}

Status RedisHashes::HMSet(const Slice& key,
                          const std::vector<FieldValue>& fvs) {
  uint32_t statistic = 0;
  std::vector<const FieldValue*> filtered_fvs;
  filtered_fvs.reserve(fvs.size());
  {
    terark::gold_hash_tab<Slice,Slice,SliceHashEqual> fields(fvs.size());
    for (auto iter = fvs.rbegin(); iter != fvs.rend(); ++iter) {
      const std::string& field = iter->field;
      if (fields.insert_i(field).second) {
        filtered_fvs.push_back(&*iter);
      }
    }
  }
  const size_t num = filtered_fvs.size();

  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.set_count(filtered_fvs.size());
      batch.Put(handles_[0], key, meta_value);
      for (const auto* pfv : filtered_fvs) {
        const auto& fv = *pfv;
        HashesDataKey hashes_data_key(key, version, fv.field);
        batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
        HashFieldAddHistogram(fv.field.size(), fv.value.size());
      }
    } else {
      int32_t count = 0;
      version = parsed_hashes_meta_value.version();
      valvec<rocksdb::PinnableSlice> values_data(num);
      valvec<Status>         status_vec(num);
      valvec<HashesDataKey>  fields_key_data(num, valvec_reserve());
      AutoFree<Slice>        fields_key_ref(num);
      for (size_t i = 0; i < num; ++i) {
        const auto& fv = *filtered_fvs[i];
        fields_key_data.unchecked_emplace_back(key, version, fv.field);
        fields_key_ref.p[i] = fields_key_data[i].Encode();
      }
      db_->MultiGet(default_read_options_, handles_[1], num,
            fields_key_ref.p, values_data.data(), status_vec.data());
      for (size_t i = 0; i < num; ++i) {
        const auto& fv = *filtered_fvs[i];
        const Slice& encoded_data_key = fields_key_ref.p[i];
        s = std::move(status_vec[i]);
        if (s.ok()) {
          statistic++;
          batch.Put(handles_[1], encoded_data_key, fv.value);
        } else if (s.IsNotFound()) {
          count++;
          batch.Put(handles_[1], encoded_data_key, fv.value);
          HashFieldAddHistogram(fv.field.size(), fv.value.size());
        } else {
          return s;
        }
      }
      parsed_hashes_meta_value.ModifyCount(count);
      batch.Put(handles_[0], key, meta_value);
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, filtered_fvs.size());
    HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    for (const auto* pfv : filtered_fvs) {
      const auto& fv = *pfv;
      HashesDataKey hashes_data_key(key, version, fv.field);
      batch.Put(handles_[1], hashes_data_key.Encode(), fv.value);
      HashFieldAddHistogram(fv.field.size(), fv.value.size());
    }
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisHashes::HSet(const Slice& key, const Slice& field,
                         const Slice& value, int32_t* res) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  uint32_t statistic = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.set_count(1);
      batch.Put(handles_[0], key, meta_value);
      HashesDataKey data_key(key, version, field);
      batch.Put(handles_[1], data_key.Encode(), value);
      HashFieldAddHistogram(field.size(), value.size());
      *res = 1;
    } else {
      version = parsed_hashes_meta_value.version();
      std::string data_value;
      HashesDataKey hashes_data_key(key, version, field);
      s = db_->Get(default_read_options_,
          handles_[1], hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *res = 0;
        if (data_value == value.ToString()) {
          return Status::OK();
        } else {
          batch.Put(handles_[1], hashes_data_key.Encode(), value);
          statistic++;
        }
      } else if (s.IsNotFound()) {
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), value);
        HashFieldAddHistogram(field.size(), value.size());
        *res = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    HashesMetaValue meta_value(std::string(str, sizeof(int32_t)));
    version = meta_value.UpdateVersion();
    batch.Put(handles_[0], key, meta_value.Encode());
    HashesDataKey data_key(key, version, field);
    batch.Put(handles_[1], data_key.Encode(), value);
    HashFieldAddHistogram(field.size(), value.size());
    *res = 1;
  } else {
    return s;
  }
  s = db_->Write(default_write_options_, &batch);
  UpdateSpecificKeyStatistics(key.ToString(), statistic);
  return s;
}

Status RedisHashes::HSetnx(const Slice& key, const Slice& field,
                           const Slice& value, int32_t* ret) {
  rocksdb::WriteBatch batch;
  ScopeRecordLock l(lock_mgr_, key);

  int32_t version = 0;
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      version = parsed_hashes_meta_value.InitialMetaValue();
      parsed_hashes_meta_value.set_count(1);
      batch.Put(handles_[0], key, meta_value);
      HashesDataKey hashes_data_key(key, version, field);
      batch.Put(handles_[1], hashes_data_key.Encode(), value);
      HashFieldAddHistogram(field.size(), value.size());
      *ret = 1;
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, field);
      std::string data_value;
      s = db_->Get(default_read_options_, handles_[1],
              hashes_data_key.Encode(), &data_value);
      if (s.ok()) {
        *ret = 0;
      } else if (s.IsNotFound()) {
        parsed_hashes_meta_value.ModifyCount(1);
        batch.Put(handles_[0], key, meta_value);
        batch.Put(handles_[1], hashes_data_key.Encode(), value);
        HashFieldAddHistogram(field.size(), value.size());
        *ret = 1;
      } else {
        return s;
      }
    }
  } else if (s.IsNotFound()) {
    char str[4];
    EncodeFixed32(str, 1);
    HashesMetaValue hashes_meta_value(std::string(str, sizeof(int32_t)));
    version = hashes_meta_value.UpdateVersion();
    batch.Put(handles_[0], key, hashes_meta_value.Encode());
    HashesDataKey hashes_data_key(key, version, field);
    batch.Put(handles_[1], hashes_data_key.Encode(), value);
    HashFieldAddHistogram(field.size(), value.size());
    *ret = 1;
  } else {
    return s;
  }
  return db_->Write(default_write_options_, &batch);
}

Status RedisHashes::HVals(const Slice& key,
                          std::vector<std::string>* values) {
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  int32_t version = 0;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_key(key, version, "");
      Slice prefix = hashes_data_key.Encode();
      auto iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(prefix);
           iter->Valid() && iter->key().starts_with(prefix);
           iter->Next()) {
        values->push_back(iter->value().ToString());
      }
      delete iter;
    }
  }
  return s;
}

Status RedisHashes::HStrlen(const Slice& key,
                            const Slice& field, int32_t* len) {
  std::string value;
  Status s = HGet(key, field, &value);
  if (s.ok()) {
    *len = value.size();
  } else {
    *len = 0;
  }
  return s;
}

Status RedisHashes::HScan(const Slice& key,
                          int64_t cursor,
                          const std::string& pattern,
                          int64_t count,
                          std::vector<FieldValue>* field_values,
                          int64_t* next_cursor) {
  *next_cursor = 0;
  field_values->clear();
  if (cursor < 0) {
    *next_cursor = 0;
    return Status::OK();
  }

  int64_t rest = count;
  int64_t step_length = count;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;

  std::string meta_value;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      *next_cursor = 0;
      return Status::NotFound();
    } else {
      std::string sub_field;
      std::string start_point;
      int32_t version = parsed_hashes_meta_value.version();
      s = GetScanStartPoint(key, pattern, cursor, &start_point);
      if (s.IsNotFound()) {
        cursor = 0;
        if (isTailWildcard(pattern)) {
          start_point = pattern.substr(0, pattern.size() - 1);
        }
      }
      if (isTailWildcard(pattern)) {
        sub_field = pattern.substr(0, pattern.size() - 1);
      }

      HashesDataKey hashes_data_prefix(key, version, sub_field);
      HashesDataKey hashes_start_data_key(key, version, start_point);
      std::string parse_key_buf;
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(hashes_start_data_key.Encode());
           iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(),
              pattern.size(), field.data(), field.size(), 0)) {
          field_values->push_back({field, iter->value().ToString()});
        }
        rest--;
      }

      if (iter->Valid()
        && (iter->key().compare(prefix) <= 0
          || iter->key().starts_with(prefix))) {
        *next_cursor = cursor + step_length;
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        std::string next_field = parsed_hashes_data_key.field().ToString();
        StoreScanNextPoint(key, pattern, *next_cursor, next_field);
      } else {
        *next_cursor = 0;
      }
      delete iter;
    }
  } else {
    *next_cursor = 0;
    return s;
  }
  return Status::OK();
}

Status RedisHashes::HScanx(const Slice& key,
                           const std::string& start_field,
                           const std::string& pattern,
                           int64_t count, std::vector<FieldValue>* field_values,
                           std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t rest = count;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;
  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      *next_field = "";
      return Status::NotFound();
    } else {
      int32_t version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_prefix(key, version, Slice());
      HashesDataKey hashes_start_data_key(key, version, start_field);
      std::string parse_key_buf;
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(hashes_start_data_key.Encode());
           iter->Valid() && rest > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        std::string field = parsed_hashes_data_key.field().ToString();
        if (StringMatch(pattern.data(),
              pattern.size(), field.data(), field.size(), 0)) {
          field_values->push_back({field, iter->value().ToString()});
        }
        rest--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        *next_field = parsed_hashes_data_key.field().ToString();
      } else {
        *next_field = "";
      }
      delete iter;
    }
  } else {
    *next_field = "";
    return s;
  }
  return Status::OK();
}

Status RedisHashes::PKHScanRange(const Slice& key,
                                 const Slice& field_start,
                                 const std::string& field_end,
                                 const Slice& pattern,
                                 int32_t limit,
                                 std::vector<FieldValue>* field_values,
                                 std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t remain = limit;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool start_no_limit = !field_start.compare("");
  bool end_no_limit = !field_end.compare("");

  if (!start_no_limit
    && !end_no_limit
    && (field_start.compare(field_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_hashes_meta_value.version();
      HashesDataKey hashes_data_prefix(key, version, Slice());
      HashesDataKey hashes_start_data_key(key, version, field_start);
      std::string parse_key_buf;
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->Seek(start_no_limit ? prefix : hashes_start_data_key.Encode());
           iter->Valid() && remain > 0 && iter->key().starts_with(prefix);
           iter->Next()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        std::string field = parsed_hashes_data_key.field().ToString();
        if (!end_no_limit && field.compare(field_end) > 0) {
          break;
        }
        if (StringMatch(pattern.data(),
              pattern.size(), field.data(), field.size(), 0)) {
          field_values->push_back({field, iter->value().ToString()});
        }
        remain--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        if (end_no_limit
          || parsed_hashes_data_key.field().compare(field_end) <= 0) {
          *next_field = parsed_hashes_data_key.field().ToString();
        }
      }
      delete iter;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisHashes::PKHRScanRange(const Slice& key,
                                  const Slice& field_start,
                                  const std::string& field_end,
                                  const Slice& pattern,
                                  int32_t limit,
                                  std::vector<FieldValue>* field_values,
                                  std::string* next_field) {
  next_field->clear();
  field_values->clear();

  int64_t remain = limit;
  std::string meta_value;
  rocksdb::ReadOptions read_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  read_options.snapshot = snapshot;

  bool start_no_limit = !field_start.compare("");
  bool end_no_limit = !field_end.compare("");

  if (!start_no_limit
    && !end_no_limit
    && (field_start.compare(field_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  Status s = db_->Get(read_options, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t version = parsed_hashes_meta_value.version();
      int32_t start_key_version = start_no_limit ? version + 1 : version;
      std::string start_key_field = start_no_limit ?
        "" : field_start.ToString();
      HashesDataKey hashes_data_prefix(key, version, Slice());
      HashesDataKey hashes_start_data_key(
          key, start_key_version, start_key_field);
      std::string parse_key_buf;
      std::string prefix = hashes_data_prefix.Encode().ToString();
      rocksdb::Iterator* iter = db_->NewIterator(read_options, handles_[1]);
      for (iter->SeekForPrev(hashes_start_data_key.Encode().ToString());
           iter->Valid() && remain > 0 && iter->key().starts_with(prefix);
           iter->Prev()) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        std::string field = parsed_hashes_data_key.field().ToString();
        if (!end_no_limit && field.compare(field_end) < 0) {
          break;
        }
        if (StringMatch(pattern.data(),
              pattern.size(), field.data(), field.size(), 0)) {
          field_values->push_back({field, iter->value().ToString()});
        }
        remain--;
      }

      if (iter->Valid() && iter->key().starts_with(prefix)) {
        ParsedHashesDataKey parsed_hashes_data_key(iter->key(), &parse_key_buf);
        if (end_no_limit
          || parsed_hashes_data_key.field().compare(field_end) >= 0) {
          *next_field = parsed_hashes_data_key.field().ToString();
        }
      }
      delete iter;
    }
  } else {
    return s;
  }
  return Status::OK();
}

Status RedisHashes::PKScanRange(const Slice& key_start,
                                const Slice& key_end,
                                const Slice& pattern,
                                int32_t limit,
                                std::vector<std::string>* keys,
                                std::string* next_key) {
  next_key->clear();

  std::string key;
  int32_t remain = limit;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  bool start_no_limit = !key_start.compare("");
  bool end_no_limit = !key_end.compare("");

  if (!start_no_limit
    && !end_no_limit
    && (key_start.compare(key_end) > 0)) {
    return Status::InvalidArgument("error in given range");
  }

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  if (start_no_limit) {
    it->SeekToFirst();
  } else {
    it->Seek(key_start);
  }

  while (it->Valid() && remain > 0
    && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedHashesMetaValue parsed_hashes_meta_value(it->value());
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      it->Next();
    } else {
      key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
      remain--;
      it->Next();
    }
  }

  while (it->Valid()
    && (end_no_limit || it->key().compare(key_end) <= 0)) {
    ParsedHashesMetaValue parsed_hashes_meta_value(it->value());
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      it->Next();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}

Status RedisHashes::PKRScanRange(const Slice& key_start,
                                 const Slice& key_end,
                                 const Slice& pattern,
                                 int32_t limit,
                                 std::vector<std::string>* keys,
                                 std::string* next_key) {
  next_key->clear();

  std::string key;
  int32_t remain = limit;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  bool start_no_limit = !key_start.compare("");
  bool end_no_limit = !key_end.compare("");

  if (!start_no_limit
    && !end_no_limit
    && (key_start.compare(key_end) < 0)) {
    return Status::InvalidArgument("error in given range");
  }

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  if (start_no_limit) {
    it->SeekToLast();
  } else {
    it->SeekForPrev(key_start);
  }

  while (it->Valid() && remain > 0
       && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedHashesMetaValue parsed_hashes_meta_value(it->value());
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      it->Prev();
    } else {
      key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         key.data(), key.size(), 0)) {
        keys->push_back(key);
      }
      remain--;
      it->Prev();
    }
  }

  while (it->Valid()
    && (end_no_limit || it->key().compare(key_end) >= 0)) {
    ParsedHashesMetaValue parsed_hashes_meta_value(it->value());
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      it->Prev();
    } else {
      *next_key = it->key().ToString();
      break;
    }
  }
  delete it;
  return Status::OK();
}


Status RedisHashes::Expire(const Slice& key, int32_t ttl) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    }

    if (ttl > 0) {
      parsed_hashes_meta_value.SetRelativeTimestamp(ttl);
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    } else {
      parsed_hashes_meta_value.InitialMetaValue();
      HashKeyHistogram(length_histogram::Del, key.size());
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisHashes::Del(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      uint32_t statistic = parsed_hashes_meta_value.count();
      parsed_hashes_meta_value.InitialMetaValue();
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
      HashKeyHistogram(length_histogram::Del, key.size());
      HashFieldDelHistogram(key.size(), meta_value.size() - data_suffix_length);
      UpdateSpecificKeyStatistics(key.ToString(), statistic);
    }
  }
  return s;
}

bool RedisHashes::Scan(const std::string& start_key,
                       const std::string& pattern,
                       std::vector<std::string>* keys,
                       int64_t* count,
                       std::string* next_key) {
  std::string meta_key;
  bool is_finish = true;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);

  it->Seek(start_key);
  while (it->Valid() && (*count) > 0) {
    ParsedHashesMetaValue parsed_meta_value(it->value());
    if (parsed_meta_value.IsStale()
      || parsed_meta_value.count() == 0) {
      it->Next();
      continue;
    } else {
      meta_key = it->key().ToString();
      if (StringMatch(pattern.data(), pattern.size(),
                         meta_key.data(), meta_key.size(), 0)) {
        keys->push_back(meta_key);
      }
      (*count)--;
      it->Next();
    }
  }

  std::string prefix = isTailWildcard(pattern) ?
    pattern.substr(0, pattern.size() - 1) : "";
  if (it->Valid()
    && (it->key().compare(prefix) <= 0 || it->key().starts_with(prefix))) {
    *next_key = it->key().ToString();
    is_finish = false;
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

bool RedisHashes::PKExpireScan(const std::string& start_key,
                               int32_t min_timestamp, int32_t max_timestamp,
                               std::vector<std::string>* keys,
                               int64_t* leftover_visits,
                               std::string* next_key) {
  bool is_finish = true;
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;

  rocksdb::Iterator* it = db_->NewIterator(iterator_options, handles_[0]);
  it->Seek(start_key);
  while (it->Valid() && (*leftover_visits) > 0) {
    ParsedHashesMetaValue parsed_hashes_meta_value(it->value());
    if (parsed_hashes_meta_value.IsStale()
      || parsed_hashes_meta_value.count() == 0) {
      it->Next();
      continue;
    } else {
      if (min_timestamp < parsed_hashes_meta_value.timestamp()
        && parsed_hashes_meta_value.timestamp() < max_timestamp) {
        keys->push_back(it->key().ToString());
      }
      (*leftover_visits)--;
      it->Next();
    }
  }

  if (it->Valid()) {
    is_finish = false;
    *next_key = it->key().ToString();
  } else {
    *next_key = "";
  }
  delete it;
  return is_finish;
}

Status RedisHashes::Expireat(const Slice& key, int32_t timestamp) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      if (timestamp > 0) {
        parsed_hashes_meta_value.set_timestamp(timestamp);
      } else {
        parsed_hashes_meta_value.InitialMetaValue();
      }
      s = db_->Put(default_write_options_, handles_[0], key, meta_value);
    }
  }
  return s;
}

Status RedisHashes::Persist(const Slice& key) {
  std::string meta_value;
  ScopeRecordLock l(lock_mgr_, key);
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      return Status::NotFound();
    } else {
      int32_t timestamp = parsed_hashes_meta_value.timestamp();
      if (timestamp == 0) {
        return Status::NotFound("Not have an associated timeout");
      }  else {
        parsed_hashes_meta_value.set_timestamp(0);
        s = db_->Put(default_write_options_, handles_[0], key, meta_value);
      }
    }
  }
  return s;
}

Status RedisHashes::TTL(const Slice& key, int64_t* timestamp) {
  std::string meta_value;
  Status s = db_->Get(default_read_options_, handles_[0], key, &meta_value);
  if (s.ok()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(&meta_value);
    if (parsed_hashes_meta_value.IsStale()) {
      *timestamp = -2;
      return Status::NotFound("Stale");
    } else if (parsed_hashes_meta_value.count() == 0) {
      *timestamp = -2;
      return Status::NotFound();
    } else {
      *timestamp = parsed_hashes_meta_value.timestamp();
      if (*timestamp == 0) {
        *timestamp = -1;
      } else {
        int64_t curtime;
        rocksdb::Env::Default()->GetCurrentTime(&curtime);
        *timestamp = *timestamp - curtime >= 0 ? *timestamp - curtime : -2;
      }
    }
  } else if (s.IsNotFound()) {
    *timestamp = -2;
  }
  return s;
}

void RedisHashes::ScanDatabase() {
  rocksdb::ReadOptions iterator_options;
  const rocksdb::Snapshot* snapshot;
  ScopeSnapshot ss(db_, &snapshot);
  iterator_options.snapshot = snapshot;
  iterator_options.fill_cache = false;
  int32_t current_time = time(NULL);

  printf("\n***************Hashes Meta Data***************\n");
  auto meta_iter = db_->NewIterator(iterator_options, handles_[0]);
  for (meta_iter->SeekToFirst();
       meta_iter->Valid();
       meta_iter->Next()) {
    ParsedHashesMetaValue parsed_hashes_meta_value(meta_iter->value());
    int32_t survival_time = 0;
    if (parsed_hashes_meta_value.timestamp() != 0) {
      survival_time = parsed_hashes_meta_value.timestamp() - current_time > 0 ?
        parsed_hashes_meta_value.timestamp() - current_time : -1;
    }

    printf("[key : %-30s] [count : %-10d] [timestamp : %-10d] [version : %d] [survival_time : %d]\n",
           meta_iter->key().ToString().c_str(),
           parsed_hashes_meta_value.count(),
           parsed_hashes_meta_value.timestamp(),
           parsed_hashes_meta_value.version(),
           survival_time);
  }
  delete meta_iter;

  printf("\n***************Hashes Field Data***************\n");
  std::string parse_key_buf;
  auto field_iter = db_->NewIterator(iterator_options, handles_[1]);
  for (field_iter->SeekToFirst();
       field_iter->Valid();
       field_iter->Next()) {
    ParsedHashesDataKey parsed_hashes_data_key(field_iter->key(), &parse_key_buf);
    printf("[key : %-30s] [field : %-20s] [value : %-20s] [version : %d]\n",
           parsed_hashes_data_key.key().ToString().c_str(),
           parsed_hashes_data_key.field().ToString().c_str(),
           field_iter->value().ToString().c_str(),
           parsed_hashes_data_key.version());
  }
  delete field_iter;
}

}  //  namespace blackwidow
