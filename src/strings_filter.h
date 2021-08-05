//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_STRINGS_FILTER_H_
#define SRC_STRINGS_FILTER_H_

#include <string>
#include <memory>

#include "src/strings_value_format.h"
#include "rocksdb/compaction_filter.h"
#include "src/debug.h"

namespace blackwidow {

export class FilterCounter;

class StringsFilter : public rocksdb::CompactionFilter {
 public:
  StringsFilter() : factory(nullptr) {}
  ~StringsFilter() { factory->local_fc += this->fc; }
  bool Filter(int level, const rocksdb::Slice& key,
              const rocksdb::Slice& value,
              std::string* new_value, bool* value_changed) const override {

    ++fc.exec_filter_times;

    int32_t cur_time = static_cast<int32_t>(unix_time);
    ParsedStringsValue parsed_strings_value(value);
    Trace("==========================START==========================");
    Trace("[StringsFilter], key: %s, value = %s, timestamp: %d, cur_time: %d",
          key.ToString().c_str(),
          parsed_strings_value.value().ToString().c_str(),
          parsed_strings_value.timestamp(),
          cur_time);

    if (parsed_strings_value.timestamp() != 0
      && parsed_strings_value.timestamp() < cur_time) {
      Trace("Drop[Stale]");
      return true;
    } else {
      Trace("Reserve");
      return false;
    }
  }
  int64_t unix_time;

  mutable FilterCounter fc;
  StringsFilterFactory* factory;

  const char* Name() const override { return "StringsFilter"; }
};

class StringsFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  StringsFilterFactory() = default;
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
    const rocksdb::CompactionFilter::Context& context) override;
  const char* Name() const override {
    return "StringsFilterFactory";
  }
  uint64_t unix_time_ = 0; // only used by compact worker

  mutable FilterCounter local_fc;
  mutable FilterCounter remote_fc;
};

}  //  namespace blackwidow
#endif  // SRC_STRINGS_FILTER_H_
