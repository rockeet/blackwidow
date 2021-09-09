// Copyright (c) 2021-present, Topling, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include <mutex>
#include "monitoring/histogram.h"

namespace db_rw_histogram{

enum data_type {
  String,
  Hash,
  List,
  Set,
  Zset,
  DBTypeMax,
};

enum process_type {
  Add,
  Del,
  ProcessTypeMax,
};

enum field_value {
  Key,
  Field,
  Value,
  FieldValueMax,
};


struct HistogramData{
  using type_table=rocksdb::HistogramStat[DBTypeMax][ProcessTypeMax][FieldValueMax];
  long check_sum;
  type_table HistogramTable;
};

class DbReadWriteHistogram {
public:
  DbReadWriteHistogram(DbReadWriteHistogram &other) = delete;
  DbReadWriteHistogram(const std::string &path);
  ~DbReadWriteHistogram();
  void Add_Histogram_Metric(const data_type type, process_type step, field_value filed, long value);
  std::string get_metric();
  void reset();

private:
  int fd;
  long get_check_sum();
  HistogramData *data;
  std::vector<std::string> const type_str{"hash","list","set","zset"};  //adpater data_type
  std::vector<std::string> const step_str{"add","del"};  //adpater process_type
  std::vector<std::string> const field_str{"field","value"};  //adpater process_type
};

} // end db_rw_histogram