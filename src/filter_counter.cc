#include "src/filter_counter.h"

namespace blackwidow {


void FilterKeysInfo::count_info(const rocksdb::Slice& key, const rocksdb::Slice& val) {
  num++;
  keys_size += key.size();
  vals_size += val.size();
}
void FilterKeysInfo::add(const FilterKeysInfo& f) {
  this->num += f.num;
  this->keys_size += f.keys_size;
  this->vals_size += f.vals_size;
}

void FilterCounter::add(const FilterCounter &f) {
  mtx.lock();
  this->exec_filter_times += f.exec_filter_times;
  this->all_retained.add(f.all_retained);
  this->deleted_not_found.add(f.deleted_not_found);
  this->deleted_expired.add(f.deleted_expired);
  this->deleted_versions_old.add(f.deleted_versions_old);
  mtx.unlock();
}

} // namespace blackwidow