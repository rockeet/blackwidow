#pragma once

#include <mutex>
#include <cstddef>
#include "rocksdb/slice.h"

namespace blackwidow {

class FilterKeysInfo {
public:
  void count_info(const rocksdb::Slice& key, const rocksdb::Slice& val);
  void add(const FilterKeysInfo&);
  size_t num = 0;
  size_t keys_size = 0, vals_size = 0;
};

class FilterCounter {
public:
  FilterCounter() = default;
  void add(const FilterCounter &f);
  size_t exec_filter_times = 0;
  FilterKeysInfo all_retained;
  FilterKeysInfo deleted_not_found;
  FilterKeysInfo deleted_expired;
  FilterKeysInfo deleted_versions_old;
private:
  std::mutex mtx;
};

} // namespace blackwidow
