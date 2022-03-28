//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "src/redis.h"
#include "topling/side_plugin_repo.h"

namespace blackwidow {

Redis::Redis(BlackWidow* const bw, const DataType& type)
    : bw_(bw),
      type_(type),
      lock_mgr_(new LockMgr(1000, 0, std::make_shared<MutexFactoryImpl>())),
      db_(nullptr),
      small_compaction_threshold_(5000) {
  statistics_store_ = new LRUCache<std::string, size_t>();
  scan_cursors_store_ = new LRUCache<std::string, std::string>();
  scan_cursors_store_->SetCapacity(5000);
  default_compact_range_options_.exclusive_manual_compaction = false;
  default_compact_range_options_.change_level = true;
  handles_.clear();
}

Redis::~Redis() {
  std::vector<rocksdb::ColumnFamilyHandle*> tmp_handles = handles_;
  handles_.clear();
  for (auto handle : tmp_handles) {
    delete handle;
  }
  delete db_;
  delete lock_mgr_;
  delete statistics_store_;
  delete scan_cursors_store_;
}

Status Redis::OpenByRepo(const BlackwidowOptions& bw_options,
                         const std::string& db_path, const std::string& type) {
  ROCKSDB_VERIFY(!bw_options.sideplugin_conf.empty());
  ROCKSDB_VERIFY(bw_->GetRepo() != nullptr);
  rocksdb::DB_MultiCF* dbm = nullptr;
  Status s = bw_->GetRepo()->OpenDB(type, &dbm);
  if (s.ok()) {
    db_ = dbm->db;
    handles_ = dbm->cf_handles;
    ROCKSDB_VERIFY_F(db_path == db_->GetName(), "type = %s : %s != %s",
       type.c_str(), db_path.c_str(), db_->GetName().c_str());
  }
  return s;
}

Status Redis::GetScanStartPoint(const Slice& key,
                                const Slice& pattern,
                                int64_t cursor,
                                std::string* start_point) {
  std::string index_key = key.ToString() + "_"
      + pattern.ToString() + "_" + std::to_string(cursor);
  return scan_cursors_store_->Lookup(index_key, start_point);
}

Status Redis::StoreScanNextPoint(const Slice& key,
                                 const Slice& pattern,
                                 int64_t cursor,
                                 const std::string& next_point) {
  std::string index_key = key.ToString() + "_"
      + pattern.ToString() +  "_" + std::to_string(cursor);
  return scan_cursors_store_->Insert(index_key, next_point);
}

Status Redis::SetMaxCacheStatisticKeys(size_t max_cache_statistic_keys) {
  statistics_store_->SetCapacity(max_cache_statistic_keys);
  return Status::OK();
}

Status Redis::SetSmallCompactionThreshold(size_t small_compaction_threshold) {
  small_compaction_threshold_ = small_compaction_threshold;
  return Status::OK();
}

Status Redis::UpdateSpecificKeyStatistics(const std::string& key,
                                          size_t count) {
  if (statistics_store_->Capacity() && count) {
    size_t total = 0;
    statistics_store_->Lookup(key, &total);
    statistics_store_->Insert(key, total + count);
    AddCompactKeyTaskIfNeeded(key, total + count);
  }
  return Status::OK();
}

Status Redis::AddCompactKeyTaskIfNeeded(const std::string& key,
                                        size_t total) {
  if (total < small_compaction_threshold_) {
    return Status::OK();
  } else {
    bw_->AddBGTask({type_, kCompactKey, key});
    statistics_store_->Remove(key);
  }
  return Status::OK();
}

Status Redis::SetOptions(const OptionType& option_type,
    const std::unordered_map<std::string, std::string>& options) {
  if (option_type == OptionType::kDB) {
    return db_->SetDBOptions(options);
  }
  if (handles_.size() == 0) {
    return db_->SetOptions(db_->DefaultColumnFamily(), options);
  }
  Status s;
  for (auto handle : handles_) {
    s = db_->SetOptions(handle, options);
    if(!s.ok()) break;
  }
  return s;
}

void Redis::GetRocksDBInfo(std::string &info, const char *prefix) {
    uint64_t backgroud_errors, compaction_pending, current_super_version_number,
      cur_size_all_mem_tables, mem_table_flush_pending, num_immutable_mem_table,
      num_live_versions,num_running_compactions, num_running_flushes,
      num_snapshots, size_all_mem_tables;

    db_->GetAggregatedIntProperty("rocksdb.background-errors", &backgroud_errors);
    db_->GetAggregatedIntProperty("rocksdb.compaction-pending", &compaction_pending);
    db_->GetAggregatedIntProperty("rocksdb.current-super-version-number", &current_super_version_number);
    db_->GetAggregatedIntProperty("rocksdb.cur-size-all-mem-tables", &cur_size_all_mem_tables);
    db_->GetAggregatedIntProperty("rocksdb.mem-table-flush-pending", &mem_table_flush_pending);
    db_->GetAggregatedIntProperty("rocksdb.num-immutable-mem-table", &num_immutable_mem_table);
    db_->GetAggregatedIntProperty("rocksdb.num-live-versions", &num_live_versions);
    db_->GetAggregatedIntProperty("rocksdb.num-running-compactions", &num_running_compactions);
    db_->GetAggregatedIntProperty("rocksdb.num-running-flushes", &num_running_flushes);
    db_->GetAggregatedIntProperty("rocksdb.num-snapshots", &num_snapshots);
    db_->GetAggregatedIntProperty("rocksdb.size-all-mem-tables", &size_all_mem_tables);

    std::ostringstream string_stream;
    string_stream << "#" << prefix << " RocksDB\r\n";
    for (const auto &cf_handle : handles_) {
      auto write_stream_int_property=[&](const char *property, const char* metric) {
        uint64_t value;
        db_->GetIntProperty(cf_handle, property, &value);
        string_stream << prefix << metric << cf_handle->GetName() << ":" << value << "\r\n";
      };

      write_stream_int_property("rocksdb.estimate-num-keys", "estimate_num_keys_");
      write_stream_int_property("rocksdb.estimate-table-readers-mem", "estimate_table_readers_mem_");

      std::map<std::string, std::string> cf_stats_map;
      auto write_stream_strings_strings=[&](const char* strkey, const char *map_key) {
        string_stream << prefix << strkey << cf_handle->GetName() << ":" << cf_stats_map[map_key] << "\r\n";
      };
      db_->GetMapProperty(cf_handle, rocksdb::DB::Properties::kCFStats, &cf_stats_map);
      write_stream_strings_strings("level0_numfiles_", "io_stalls.level0_numfiles");
      write_stream_strings_strings("level0_slowdown_", "io_stalls.level0_slowdown");
      write_stream_strings_strings("memtable_compaction_", "io_stalls.memtable_compaction");
      write_stream_strings_strings("memtable_slowdown_", "io_stalls.memtable_slowdown");
      write_stream_strings_strings("stop_for_pending_compaction_bytes_", "io_stalls.stop_for_pending_compaction_bytes");
      write_stream_strings_strings("slowdown_for_pending_compaction_bytes_", "io_stalls.slowdown_for_pending_compaction_bytes");
    }

    auto write_stream_key_value=[&](const char *key, const u_int64_t value) {
      string_stream << prefix << key << value << "\r\n";
    };
    write_stream_key_value("all_mem_tables:", size_all_mem_tables);
    write_stream_key_value("compaction_pending:", compaction_pending);
    write_stream_key_value("cur_mem_tables:", cur_size_all_mem_tables);
    write_stream_key_value("current_super_version_number:", current_super_version_number);
    write_stream_key_value("mem_table_flush_pending:", mem_table_flush_pending);
    write_stream_key_value("num_background_errors:", backgroud_errors);
    write_stream_key_value("num_immutable_mem_table:", num_immutable_mem_table);
    write_stream_key_value("num_live_versions:", num_live_versions);
    write_stream_key_value("num_running_compactions:", num_running_compactions);
    write_stream_key_value("num_running_flushes:", num_running_flushes);
    write_stream_key_value("snapshots:", num_snapshots);
    info.append(string_stream.str());
  }

}  // namespace blackwidow
