//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_ZSETS_DATA_KEY_FORMAT_H_
#define SRC_ZSETS_DATA_KEY_FORMAT_H_

#include "base_data_key_format.h"

namespace blackwidow {

/*
 * |  <Key Size>  |      <Key>      | <Version> |  <Score>  |      <Member>      |
 *      4 Bytes      key size Bytes    4 Bytes     8 Bytes    member size Bytes
 */
class ZSetsScoreKey {
 public:
  ZSetsScoreKey(const Slice& key, int32_t version,
                double score, const Slice& member) :
    start_(nullptr), key_(key),
    version_(version), score_(score),
    member_(member) {}

  ~ZSetsScoreKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice Encode() {
#ifdef TOPLING_KEY_FORMAT
    size_t ksize = key_.size_;
    size_t nzero = std::count(key_.begin(), key_.end(), 0);
    size_t needed = key_.size_ + member_.size_
                        + nzero + 2
                        + sizeof(int32_t) * 1 + sizeof(uint64_t);
#else
    size_t needed = key_.size() + member_.size()
                        + sizeof(int32_t) * 2 + sizeof(uint64_t);
#endif
    char* dst = nullptr;
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
    dst = encode_0_01_00(key_.data_, key_.end(), dst, dst+ksize+nzero+2);
    ROCKSDB_VERIFY_EQ(size_t(dst-start_), ksize+nzero+2);
    unaligned_save(dst, BIG_ENDIAN_OF(version_));
    dst = (char*)encode_memcmp_double(score_, (unsigned char*)dst + 4);
#else
    EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    const void* addr_score = reinterpret_cast<const void*>(&score_);
    EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_score));
    dst += sizeof(uint64_t);
#endif
    memcpy(dst, member_.data(), member_.size());
    return Slice(start_, needed);
  }

 private:
  char space_[200];
  char* start_;
  Slice key_;
  int32_t version_;
  double score_;
  Slice member_;
};


class ParsedZSetsScoreKey {
 public:
  explicit ParsedZSetsScoreKey(std::string* key_parse_inplace)
    : ParsedZSetsScoreKey(*key_parse_inplace, key_parse_inplace) {}
  ParsedZSetsScoreKey(Slice key, std::string* parse_buf) {
    const char* ptr = key.data();
#ifdef TOPLING_KEY_FORMAT
    ROCKSDB_VERIFY_GE(key.size_, 2);
    size_t cap = key.size_ - 2;
    parse_buf->resize(cap);
    char* obeg = parse_buf->data();
    char* oend = decode_01_00(ptr, &ptr, obeg, obeg + cap);
    ROCKSDB_VERIFY_LT(size_t(ptr - key.data_), key.size_);
    ROCKSDB_VERIFY_EQ(ptr[-1], 0);
    key_ = Slice(obeg, oend - obeg);
    version_ = NATIVE_OF_BIG_ENDIAN(unaligned_load<int32_t>(ptr));
    ptr = (char*)decode_memcmp_double((unsigned char*)ptr + 4, &score_);
#else
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    score_ = *reinterpret_cast<const double*>(ptr_tmp);
    ptr += sizeof(uint64_t);
#endif
    member_ = Slice(ptr, key.end() - ptr);
  }

#ifndef TOPLING_KEY_FORMAT
  explicit ParsedZSetsScoreKey(const Slice& key) {
    const char* ptr = key.data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    uint64_t tmp = DecodeFixed64(ptr);
    const void* ptr_tmp = reinterpret_cast<const void*>(&tmp);
    score_ = *reinterpret_cast<const double*>(ptr_tmp);
    ptr += sizeof(uint64_t);
    member_ = Slice(ptr, key.size() - key_len
                       - 2 * sizeof(int32_t) - sizeof(uint64_t));
  }
#endif

  Slice key() {
    return key_;
  }
  int32_t version() const {
    return version_;
  }
  double score() const {
    return score_;
  }
  Slice member() {
    return member_;
  }

 private:
  Slice key_;
  int32_t version_;
  double score_;
  Slice member_;
};

}  // namespace blackwidow
#endif  // SRC_ZSETS_DATA_KEY_FORMAT_H_
