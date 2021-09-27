#pragma once
#include <terark/gold_hash_map.hpp>
#include <rocksdb/slice.h>
#include <zstd/common/xxhash.h>

struct SliceHashEqual {
  inline size_t hash(const rocksdb::Slice& x) const {
    return (size_t)XXH64(x.data_, x.size_, 202109161242ULL);
  }
  inline bool equal(const rocksdb::Slice& x, const rocksdb::Slice& y) const {
    return x == y;
  }
};

