#pragma once

#include <cstddef>
#include "rocksdb/compaction_filter.h"

namespace blackwidow {

#define Add_and_Destructor_Mutex(factory, Summand_fc, n, Addend_fc) \
factory->mtx[n].lock(); \
factory->Summand_fc.add(Addend_fc); \
factory->mtx[n].unlock();

class FilterCounter {
public:
  FilterCounter();
  void add(const FilterCounter &f);
  void count_reserved_kv(const rocksdb::Slice& key, const rocksdb::Slice& value);
  void count_deleted_kv(const rocksdb::Slice& key, const rocksdb::Slice& value);
  size_t exec_filter_times;
  size_t total_reserved_kv_num;
  size_t total_reserved_keys_size, total_reserved_vals_size;
  size_t deleted_not_found_keys_num;
  size_t deleted_expired_keys_num;
  size_t deleted_versions_old_keys_num;
  size_t total_deleted_keys_size, total_deleted_vals_size;
};

} // namespace blackwidow
