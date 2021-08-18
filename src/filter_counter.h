#pragma once

#include <mutex>
#include <cstddef>
#include "rocksdb/slice.h"

namespace blackwidow {

class FilterCounter {
public:
  FilterCounter() = default;
  void add(const FilterCounter &f);
  void count_reserved_kv(const rocksdb::Slice& key, const rocksdb::Slice& value);
  void count_deleted_kv(const rocksdb::Slice& key, const rocksdb::Slice& value);
  size_t exec_filter_times = 0;
  size_t total_reserved_kv_num = 0;
  size_t total_reserved_keys_size, total_reserved_vals_size = 0;
  size_t deleted_not_found_keys_num = 0;
  size_t deleted_expired_keys_num = 0;
  size_t deleted_versions_old_keys_num = 0;
  size_t total_deleted_keys_size, total_deleted_vals_size = 0;
private:
  std::mutex mtx;
};

} // namespace blackwidow
