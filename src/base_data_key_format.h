//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_BASE_DATA_KEY_FORMAT_H_
#define SRC_BASE_DATA_KEY_FORMAT_H_

#include "coding.h"

namespace blackwidow {

class BaseDataKey {
 public:
  BaseDataKey(const Slice& key, int32_t version, const Slice& data) :
    start_(nullptr), key_(key), data_(data), version_(version) {}

  ~BaseDataKey() {
    if (start_ != space_) {
      delete[] start_;
    }
  }

  const Slice Encode() {
    size_t usize = key_.size() + data_.size();
#ifdef TOPLING_KEY_FORMAT
    size_t ksize = key_.size_;
    size_t nzero = std::count(key_.begin(), key_.end(), 0);
    size_t needed = usize + nzero + 2 + sizeof(int32_t);
#else
    size_t needed = usize + sizeof(int32_t) * 2;
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
    dst = encode_00_0n(key_.data_, key_.end(), dst, dst+ksize+nzero+2, 1);
    ROCKSDB_VERIFY_EQ(size_t(dst-start_), ksize+nzero+2);
#else
    EncodeFixed32(dst, key_.size());
    dst += sizeof(int32_t);
    memcpy(dst, key_.data(), key_.size());
    dst += key_.size();
#endif
    EncodeFixed32(dst, version_);
    dst += sizeof(int32_t);
    memcpy(dst, data_.data(), data_.size());
    return Slice(start_, needed);
  }

 private:
  char* start_;
  Slice key_;
  Slice data_;
  int32_t version_;
  char space_[124];
};

class ParsedBaseDataKey {
 public:
  explicit ParsedBaseDataKey(std::string* key_parse_inplace)
    : ParsedBaseDataKey(*key_parse_inplace, key_parse_inplace) {}
  ParsedBaseDataKey(Slice key, std::string* parse_buf) {
    const char* ptr = key.data();
#ifdef TOPLING_KEY_FORMAT
    size_t cap = key.size_;
    parse_buf->resize(cap);
    char* obeg = parse_buf->data();
    char* oend = decode_00_0n(ptr, &ptr, obeg, obeg + cap);
    ROCKSDB_VERIFY_LT(size_t(ptr - key.data_), key.size_);
    key_ = Slice(obeg, oend - obeg - 2);
#else
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
#endif
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    data_ = Slice(ptr, key.end() - ptr);
  }

#ifndef TOPLING_KEY_FORMAT
  explicit ParsedBaseDataKey(const Slice& key) {
    const char* ptr = key.data();
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    key_ = Slice(ptr, key_len);
    ptr += key_len;
    version_ = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);
    data_ = Slice(ptr, key.size() - key_len - sizeof(int32_t) * 2);
  }
#endif

  virtual ~ParsedBaseDataKey() = default;

  Slice key() {
    return key_;
  }

  int32_t version() {
    return version_;
  }

  Slice data() {
    return data_;
  }

 protected:
  Slice key_;
  int32_t version_;
  Slice data_;
};

class ParsedHashesDataKey : public ParsedBaseDataKey {
 public:
#ifdef TOPLING_KEY_FORMAT
  using ParsedBaseDataKey::ParsedBaseDataKey;
#else
  explicit ParsedHashesDataKey(const std::string* key)
              : ParsedBaseDataKey(key) {}
  explicit ParsedHashesDataKey(const Slice& key)
              : ParsedBaseDataKey(key) {}
#endif
  Slice field() {
    return data_;
  }
};

class ParsedSetsMemberKey : public ParsedBaseDataKey {
 public:
#ifdef TOPLING_KEY_FORMAT
  using ParsedBaseDataKey::ParsedBaseDataKey;
#else
  explicit ParsedSetsMemberKey(const std::string* key)
              : ParsedBaseDataKey(key) {}
  explicit ParsedSetsMemberKey(const Slice& key)
              : ParsedBaseDataKey(key) {}
#endif
  Slice member() {
    return data_;
  }
};

class ParsedZSetsMemberKey : public ParsedBaseDataKey {
 public:
#ifdef TOPLING_KEY_FORMAT
  using ParsedBaseDataKey::ParsedBaseDataKey;
#else
  explicit ParsedZSetsMemberKey(const std::string* key)
              : ParsedBaseDataKey(key) {}
  explicit ParsedZSetsMemberKey(const Slice& key)
              : ParsedBaseDataKey(key) {}
#endif
  Slice member() {
    return data_;
  }
};

typedef BaseDataKey HashesDataKey;
typedef BaseDataKey SetsMemberKey;
typedef BaseDataKey ZSetsMemberKey;

}  //  namespace blackwidow
#endif  // SRC_BASE_DATA_KEY_FORMAT_H_
