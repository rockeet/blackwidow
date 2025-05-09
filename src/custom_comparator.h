//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef INCLUDE_CUSTOM_COMPARATOR_H_
#define INCLUDE_CUSTOM_COMPARATOR_H_
#include "string"

#include "src/coding.h"

namespace blackwidow {

class ListsDataKeyComparatorImpl : public rocksdb::Comparator {
 public:
  ListsDataKeyComparatorImpl() { }

  const char* Name() const override {
    return "blackwidow.ListsDataKeyComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    assert(!a.empty() && !b.empty());
    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    int32_t a_size = static_cast<int32_t>(a.size());
    int32_t b_size = static_cast<int32_t>(b.size());
  #ifdef TOPLING_KEY_FORMAT
    const char*  kend_a = end_of_01_00(ptr_a);
    const char*  kend_b = end_of_01_00(ptr_b);
    const size_t klen_a = kend_a - ptr_a;
    const size_t klen_b = kend_b - ptr_b;
    ROCKSDB_VERIFY_EQ(klen_a + 12, a.size_);
    ROCKSDB_VERIFY_EQ(klen_b + 12, b.size_);
    auto minlen = std::min(klen_a, klen_b);
    auto cmpret = memcmp(ptr_a, ptr_b, minlen);
    if (cmpret)
      return cmpret;
    if (klen_a != klen_b)
      return int(klen_a) - int(klen_b); // key len must < INT_MAX
    ptr_a = kend_a;
    ptr_b = kend_b;
  #else
    int32_t key_a_len = DecodeFixed32(ptr_a);
    int32_t key_b_len = DecodeFixed32(ptr_b);
    ptr_a += sizeof(int32_t);
    ptr_b += sizeof(int32_t);
    Slice sets_key_a(ptr_a,  key_a_len);
    Slice sets_key_b(ptr_b,  key_b_len);
    ptr_a += key_a_len;
    ptr_b += key_b_len;
    if (sets_key_a != sets_key_b) {
      return sets_key_a.compare(sets_key_b);
    }
    if (ptr_a - a.data() == a_size &&
      ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }
#endif

    int32_t version_a = DecodeFixed32(ptr_a);
    int32_t version_b = DecodeFixed32(ptr_b);
    ptr_a += sizeof(int32_t);
    ptr_b += sizeof(int32_t);
    if (version_a != version_b) {
      return version_a < version_b ? -1 : 1;
    }
    if (ptr_a - a.data() == a_size &&
      ptr_b - b.data() == b_size) {
      return 0;
    } else if (ptr_a - a.data() == a_size) {
      return -1;
    } else if (ptr_b - b.data() == b_size) {
      return 1;
    }

    uint64_t index_a = DecodeFixed64(ptr_a);
    uint64_t index_b = DecodeFixed64(ptr_b);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (index_a != index_b) {
      return index_a < index_b ? -1 : 1;
    } else {
      return 0;
    }
  }

  bool Equal(const Slice& a, const Slice& b) const override {
    return !Compare(a, b);
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
  }

  void FindShortSuccessor(std::string* key) const override {
  }
};


/*
 *  |  <Key Size>  |      <Key>      |  <Version>  |  <Score>  | <Member> |
 *       4 Bytes     Key Size Bytes      4 Bytes      8 Bytes
 */
class ZSetsScoreKeyComparatorImpl : public rocksdb::Comparator {
 public:
  const char* Name() const override {
    return "blackwidow.ZSetsScoreKeyComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
#ifndef TOPLING_KEY_FORMAT
    assert(a.size() > sizeof(int32_t));
    assert(a.size() >= DecodeFixed32(a.data())
            + 2 * sizeof(int32_t) + sizeof(uint64_t));
    assert(b.size() > sizeof(int32_t));
    assert(b.size() >= DecodeFixed32(b.data())
            + 2 * sizeof(int32_t) + sizeof(uint64_t));
#endif

    const char* ptr_a = a.data();
    const char* ptr_b = b.data();
    int32_t a_size = static_cast<int32_t>(a.size());
    int32_t b_size = static_cast<int32_t>(b.size());
#ifdef TOPLING_KEY_FORMAT
    const char*  kend_a = end_of_01_00(ptr_a);
    const char*  kend_b = end_of_01_00(ptr_b);
    const size_t klen_a = kend_a - ptr_a + 4; // 4B is version
    const size_t klen_b = kend_b - ptr_b + 4;
    ROCKSDB_VERIFY_LE(klen_a + 8, a.size_);
    ROCKSDB_VERIFY_LE(klen_b + 8, b.size_);
    // key + version is bytewise compare
    auto minlen = std::min(klen_a, klen_b);
    auto ret = memcmp(ptr_a, ptr_b, minlen);
    if (ret)
      return ret;
    if (klen_a != klen_b)
      return int(klen_a) - int(klen_b); // key len must < INT_MAX
    ptr_a = kend_a;
    ptr_b = kend_b;
#else
    int32_t key_a_len = DecodeFixed32(ptr_a);
    int32_t key_b_len = DecodeFixed32(ptr_b);
    Slice key_a_prefix(ptr_a,  key_a_len + 2 * sizeof(int32_t));
    Slice key_b_prefix(ptr_b,  key_b_len + 2 * sizeof(int32_t));
    ptr_a += key_a_len + 2 * sizeof(int32_t);
    ptr_b += key_b_len + 2 * sizeof(int32_t);
    int ret = key_a_prefix.compare(key_b_prefix);
    if (ret) {
      return ret;
    }
#endif

    uint64_t a_i = DecodeFixed64(ptr_a);
    uint64_t b_i = DecodeFixed64(ptr_b);
    const void* ptr_a_score = reinterpret_cast<const void*>(&a_i);
    const void* ptr_b_score = reinterpret_cast<const void*>(&b_i);
    double a_score = *reinterpret_cast<const double*>(ptr_a_score);
    double b_score = *reinterpret_cast<const double*>(ptr_b_score);
    ptr_a += sizeof(uint64_t);
    ptr_b += sizeof(uint64_t);
    if (a_score != b_score) {
      return a_score < b_score  ? -1 : 1;
    } else {
      if (ptr_a - a.data() == a_size && ptr_b - b.data() == b_size) {
        return 0;
      } else if (ptr_a - a.data() == a_size) {
        return -1;
      } else if (ptr_b - b.data() == b_size) {
        return 1;
      } else {
        Slice key_a_member(ptr_a, a_size - (ptr_a - a.data()));
        Slice key_b_member(ptr_b, b_size - (ptr_b - b.data()));
        ret = key_a_member.compare(key_b_member);
        if (ret) {
          return ret;
        }
      }
    }
    return 0;
  }

  bool Equal(const Slice& a, const Slice& b) const override {
    return !Compare(a, b);
  }

  void ParseAndPrintZSetsScoreKey(const std::string& from, const std::string& str) {
    const char* ptr = str.data();

#ifdef TOPLING_KEY_FORMAT
    std::string key(str.size() - 4 - 8 - 2, '\0');
    char* dk_end = decode_01_00(ptr, &ptr, &key[0], &key[0] + key.size());
    ROCKSDB_VERIFY_LE(size_t(ptr - str.data()), str.size() - 4 - 8);
    ROCKSDB_VERIFY_EQ(ptr[-1], 0);
    int32_t key_len = int32_t(dk_end - &key[0]);
    key.resize(key_len);
#else
    int32_t key_len = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    std::string key(ptr, key_len);
    ptr += key_len;
#endif

    int32_t version = DecodeFixed32(ptr);
    ptr += sizeof(int32_t);

    uint64_t key_score_i = DecodeFixed64(ptr);
    const void* ptr_key_score = reinterpret_cast<const void*>(&key_score_i);
    double score = *reinterpret_cast<const double*>(ptr_key_score);
    ptr += sizeof(uint64_t);


    std::string member(ptr, str.data() + str.size());
    printf("%s: total_len[%lu], key_len[%d], key[%s], version[%d], score[%lf], member[%s]\n",
            from.data(), str.size(), key_len, key.data(), version, score, member.data());
  }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
#ifndef TOPLING_KEY_FORMAT
    assert(start->size() > sizeof(int32_t));
    assert(start->size() >= DecodeFixed32(start->data())
            + 2 * sizeof(int32_t) + sizeof(uint64_t));
    assert(limit.size() > sizeof(int32_t));
    assert(limit.size() >= DecodeFixed32(limit.data())
            + 2 * sizeof(int32_t) + sizeof(uint64_t));

    const char* ptr_start = start->data();
    const char* ptr_limit = limit.data();
    int32_t key_start_len = DecodeFixed32(ptr_start);
    int32_t key_limit_len = DecodeFixed32(ptr_limit);
    Slice key_start_prefix(ptr_start,  key_start_len + 2 * sizeof(int32_t));
    Slice key_limit_prefix(ptr_limit,  key_limit_len + 2 * sizeof(int32_t));
    ptr_start += key_start_len + 2 * sizeof(int32_t);
    ptr_limit += key_limit_len + 2 * sizeof(int32_t);
    if (key_start_prefix.compare(key_limit_prefix)) {
      return;
    }

    uint64_t start_i = DecodeFixed64(ptr_start);
    uint64_t limit_i = DecodeFixed64(ptr_limit);
    const void* ptr_start_score = reinterpret_cast<const void*>(&start_i);
    const void* ptr_limit_score = reinterpret_cast<const void*>(&limit_i);
    double start_score = *reinterpret_cast<const double*>(ptr_start_score);
    double limit_score = *reinterpret_cast<const double*>(ptr_limit_score);
    ptr_start += sizeof(uint64_t);
    ptr_limit += sizeof(uint64_t);
    if (start_score < limit_score) {
      if (start_score + 1 < limit_score) {
        start->resize(key_start_len + 2 * sizeof(int32_t));
        start_score += 1;
        const void* addr_start_score = reinterpret_cast<const void*>(&start_score);
        char dst[sizeof(uint64_t)];
        EncodeFixed64(dst, *reinterpret_cast<const uint64_t*>(addr_start_score));
        start->append(dst, sizeof(uint64_t));
      }
      return;
    }

    std::string key_start_member(ptr_start, start->size() - (key_start_len + 2 * sizeof(int32_t) + sizeof(uint64_t)));
    std::string key_limit_member(ptr_limit, limit.size() - (key_limit_len + 2 * sizeof(int32_t) + sizeof(uint64_t)));
    // Find length of common prefix
    size_t min_length = std::min(key_start_member.size(), key_limit_member.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           (key_start_member[diff_index] == key_limit_member[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t key_start_member_byte = static_cast<uint8_t>(key_start_member[diff_index]);
      uint8_t key_limit_member_byte = static_cast<uint8_t>(key_limit_member[diff_index]);
      if (key_start_member_byte >= key_limit_member_byte) {
        // Cannot shorten since limit is smaller than start or start is
        // already the shortest possible.
        return;
      }
      assert(key_start_member_byte < key_limit_member_byte);

      if (diff_index < key_limit_member.size() - 1
        || key_start_member_byte + 1 < key_limit_member_byte) {
        key_start_member[diff_index]++;
        key_start_member.resize(diff_index + 1);
        start->resize(key_start_len + 2 * sizeof(int32_t) + sizeof(uint64_t));
        start->append(key_start_member);
      } else {
        //     v
        // A A 1 A A A
        // A A 2
        //
        // Incrementing the current byte will make start bigger than limit, we
        // will skip this byte, and find the first non 0xFF byte in start and
        // increment it.
        diff_index++;

        while (diff_index < key_start_member.size()) {
          // Keep moving until we find the first non 0xFF byte to
          // increment it
          if (static_cast<uint8_t>(key_start_member[diff_index]) < static_cast<uint8_t>(0xff)) {
              key_start_member[diff_index]++;
              key_start_member.resize(diff_index + 1);
              start->resize(key_start_len + 2 * sizeof(int32_t) + sizeof(uint64_t));
              start->append(key_start_member);
              break;
          }
          diff_index++;
        }
      }
    }
#endif
  }

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  void FindShortSuccessor(std::string* key) const override {
  }
};

}  //  namespace blackwidow
#endif  //  INCLUDE_CUSTOM_COMPARATOR_H_
