//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LISTS_DATA_KEY_FORMAT_H_
#define SRC_LISTS_DATA_KEY_FORMAT_H_

#include <string>
#include "base_data_key_format.h"

namespace blackwidow {
class ListsDataKey {
 public:
  ListsDataKey(const Slice& key, int32_t version, uint64_t index) :
    start_(nullptr), key_(key), version_(version), index_(index) {
  }

  ~ListsDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice Encode() {
    size_t usize = key_.size();
#ifdef TOPLING_KEY_FORMAT
    size_t nzero = std::count(key_.begin(), key_.end(), 0);
    size_t needed = usize + nzero + 2 + sizeof(int32_t) + sizeof(uint64_t);
#else
    size_t needed = usize + sizeof(int32_t) * 2 + sizeof(uint64_t);
#endif
    char* dst;
    if (needed <= sizeof(space_)) {
      dst = space_;
    } else {
      dst = new char[needed];

      // Need to allocate space, delete previous space
      if (start_ != space_) {
        delete[] start_;
      }
    }
    start_ = dst;
#ifdef TOPLING_KEY_FORMAT
    dst = encode_0_01_00(key_.data_, key_.end(), dst, dst+usize+nzero+2);
    ROCKSDB_VERIFY_EQ(size_t(dst-start_), usize+nzero+2);
    unaligned_save(dst + 0, BIG_ENDIAN_OF(version_));
    unaligned_save(dst + 4, BIG_ENDIAN_OF(index_));
#else
    EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    EncodeFixed64(dst, index_);
#endif
    return Slice(start_, needed);
  }

 private:
  char space_[200];
  char* start_;
  Slice key_;
  int32_t version_;
  uint64_t index_;
};

class ParsedListsDataKey {
 public:
  explicit ParsedListsDataKey(std::string* key_parse_inplace)
    : ParsedListsDataKey(*key_parse_inplace, key_parse_inplace) {}
  ParsedListsDataKey(Slice key, std::string* parse_buf) {
    const char* ptr = key.data();
#ifdef TOPLING_KEY_FORMAT
    size_t cap = key.size_ - 2;
    parse_buf->resize(cap);
    char* obeg = parse_buf->data();
    char* oend = decode_01_00(ptr, &ptr, obeg, obeg + cap);
    ROCKSDB_VERIFY_LT(size_t(ptr - key.data_), key.size_);
    ROCKSDB_VERIFY_EQ(ptr[-1], 0);
    key_ = Slice(obeg, oend - obeg);
    version_ = NATIVE_OF_BIG_ENDIAN(unaligned_load<int32_t>(ptr));
    index_ = NATIVE_OF_BIG_ENDIAN(unaligned_load<uint64_t>(ptr+4));
#else
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    index_ = DecodeFixed64(ptr);
#endif
  }

#ifndef TOPLING_KEY_FORMAT
  explicit ParsedListsDataKey(const Slice& key) {
    const char* ptr = key.data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    index_ = DecodeFixed64(ptr);
  }
#endif

  Slice key() {
    return key_;
  }

  int32_t version() {
    return version_;
  }

  uint64_t index() {
    return index_;
  }

 private:
  Slice key_;
  int32_t version_;
  uint64_t index_;
};

}  //  namespace blackwidow
#endif  // SRC_LISTS_DATA_KEY_FORMAT_H_
