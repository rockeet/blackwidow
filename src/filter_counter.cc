#include "src/filter_counter.h"

namespace blackwidow {

void FilterCounter::add(const FilterCounter &f) {
  mtx.lock();
  this->exec_filter_times += f.exec_filter_times;
  this->total_reserved_kv_num += f.total_reserved_kv_num;
  this->total_reserved_keys_size += f.total_reserved_keys_size;
  this->total_reserved_vals_size += f.total_reserved_vals_size;
  this->deleted_not_found_keys_num += f.deleted_not_found_keys_num;
  this->deleted_expired_keys_num += f.deleted_expired_keys_num;
  this->deleted_versions_old_keys_num += f.deleted_versions_old_keys_num;
  this->total_deleted_keys_size += f.total_deleted_keys_size;
  this->total_deleted_vals_size += f.total_deleted_vals_size;
  mtx.unlock();
}

void FilterCounter::count_reserved_kv(const rocksdb::Slice& key, const rocksdb::Slice& value) {
  ++total_reserved_kv_num;
  total_reserved_keys_size += key.size();
  total_reserved_vals_size += value.size();
}
void FilterCounter::count_deleted_kv(const rocksdb::Slice& key, const rocksdb::Slice& value) {
  total_deleted_keys_size += key.size();
  total_deleted_vals_size += value.size();
}

} // namespace blackwidow