// Copyright (c) 2021-present, Topling, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include <mutex>
#include "monitoring/histogram.h"
#include "terark/fstring.hpp"

namespace length_histogram {

enum RedisDataType {
  Redis_String,
  Redis_Hash,
  Redis_List,
  Redis_Set,
  Redis_Zset,
  RedisTypeMax,
};

enum ProcessType {
  Add,
  Del,
  ProcessTypeMax,
};

enum FieldValue {
  Key,
  Field,
  Value,
  FieldValueMax,
};

struct HistogramData{
  using type_table=rocksdb::HistogramStat[RedisTypeMax][ProcessTypeMax][FieldValueMax];
  long check_sum;
  type_table HistogramTable;
};

class CmdDataLengthHistogram {
public:
  CmdDataLengthHistogram(CmdDataLengthHistogram &other) = delete;
  CmdDataLengthHistogram(const std::string &path);
  ~CmdDataLengthHistogram();
  void AddLengthMetric(const RedisDataType type, ProcessType step, FieldValue filed, long value);
  std::string GetLengthMetric();
  std::string GetLengthHtml();
  void Reset();

private:
  int fd;
  long GetCheckSum();
  HistogramData *data;
  terark::fstring const type_str[RedisTypeMax] = {"string","hash","list","set","zset"};
  terark::fstring const step_str[ProcessTypeMax] = {"add","del"};
  terark::fstring const field_str[FieldValueMax] = {"key","field","value"};
};

} // end length_histogram
