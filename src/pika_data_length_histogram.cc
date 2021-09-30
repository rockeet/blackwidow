// Copyright (c) 2021-present, Topling, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <unistd.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <glog/logging.h>
#include <iostream>
#include <sys/stat.h>

#include "terark/num_to_str.hpp"
#include "slash/include/env.h"
#include "pika_data_length_histogram.h"

using std::cout;
using std::endl;

namespace length_histogram {

static const rocksdb::HistogramBucketMapper bucketMapper;

void CmdDataLengthHistogram::Reset() {
  data->check_sum = 0;
  for (int i = 0; i < RedisTypeMax; i++) {
    for (int j = 0; j < ProcessTypeMax; j++) {
      for (int k = 0; k < FieldValueMax; k++) { data->HistogramTable[i][j][k].Clear(); }
    }
  }
}
// checksum 校验文件
CmdDataLengthHistogram::CmdDataLengthHistogram(const std::string &path) {
  bool exist = slash::FileExists(path);
  if (!exist) {
    fd = open(path.c_str(), O_RDWR|O_CREAT, 0644);
    ftruncate(fd, sizeof(*data));
  } else {
    fd = open(path.c_str(), O_RDWR);
  }
  if (fd < 0) LOG(FATAL) << "CmdDataLengthHistogram error fd:" << fd;

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result != 0) LOG(FATAL) << "CmdDataLengthHistogram get file size error:" << errno;
  if (buf.st_size != sizeof(*data)) {
    LOG(FATAL) << "CmdDataLengthHistogram file:"<< path << " size error current:" << buf.st_size << "!=" << sizeof(*data);
  }

  auto addr = mmap(NULL, sizeof(*data), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) LOG(FATAL) << "CmdDataLengthHistogram mmap failed";
  data = (HistogramData*)addr;

  if (exist && data->check_sum != GetCheckSum()) {
    LOG(ERROR) << "CmdDataLengthHistogram check sum failed";
  }

  if (!exist) Reset();
}

CmdDataLengthHistogram::~CmdDataLengthHistogram() {
  data->check_sum = GetCheckSum();
  munmap((void*)data, sizeof(*data));
  data = nullptr;
  close(fd);
}

long CmdDataLengthHistogram::GetCheckSum() {
  //待实现具体内容
  return 0;
}

void CmdDataLengthHistogram::AddLengthMetric(const RedisDataType type, ProcessType step, FieldValue field, long value) {
  assert(type<RedisTypeMax);
  assert(step<ProcessTypeMax);
  data->HistogramTable[type][step][field].Add(value);
}

std::string CmdDataLengthHistogram::GetLengthMetric() {
  std::ostringstream oss;
  for (int type = 0; type < RedisTypeMax; type++) {
    for(int step = 0; step < ProcessTypeMax; step++) {
      for(int field = 0; field < FieldValueMax; field++) {
        auto &buckets =  data->HistogramTable[type][step][field].buckets_;
        u_int64_t last = 0;
        size_t limit = 0;
        for (size_t i = bucketMapper.BucketCount() - 1; i > 0; i--) {
          if (buckets[i].cnt > 0) { limit = i; break; }
        }
        auto &t_name = type_str[type];
        auto &s_name = step_str[step];
        auto &f_name = field_str[field];
        auto add_label=[&t_name,&oss,&s_name,&f_name]() {
          oss<<"dbtype=\""<<t_name<<"\" step=\""<<s_name<<"\" field=\""<<f_name<<"\"";
          return "";
        };
        for (size_t i = 0; i <= limit; i++) {
          last += buckets[i].cnt;
          oss<<"pika_db_read_write_bucket{" <<add_label()<<" le=\""<<bucketMapper.BucketLimit(i)<<"\"} "<<last<<"\n";
        }
        oss<<"pika_db_read_write_bucket{"<<add_label()<<" le=\"+Inf\"} "<<last<<"\n";
        oss<<"pika_db_read_write_count{"<<add_label()<<"} "<<last<<"\n";
        oss<<"pika_db_read_write_sum{"<<add_label()<<"} "<<data->HistogramTable[type][step][field].sum_<<"\n";
      }
    }
  }

  return oss.str();
}

std::string CmdDataLengthHistogram::GetLengthHtml() {
  terark::string_appender<> oss;
  oss<<"<tr><td>";
  oss<<"<table border=1><tbody>";
  oss<<"<tr><th>type</th><th>P50</th><th>P95</th><th>P99</th><th>AVG</th><th>MIN</th><th>MAX</th><th>CNT</th><th>STD</th><th>SUM</th></tr>";
  for (int type = 0; type < RedisTypeMax; type++) {
    for (int step = 0; step < ProcessTypeMax; step++) {
      for (int field = 0; field < FieldValueMax; field++) {
        auto &histogram = data->HistogramTable[type][step][field];

        oss << "<tr>";
        oss << "<td>" << type_str[type] << ":" << step_str[step] << ":" << field_str[field] << "</td>";
        oss << "<td>" << histogram.Percentile(50) << "</td>";
        oss << "<td>" << histogram.Percentile(95) << "</td>";
        oss << "<td>" << histogram.Percentile(99) << "</td>";
        oss << "<td>" << histogram.Average() << "</td>";
        oss << "<td>" << histogram.min() << "</td>";
        oss << "<td>" << histogram.max() << "</td>";
        oss << "<td>" << histogram.num() << "</td>";
        oss << "<td>" << histogram.StandardDeviation() << "</td>";
        oss << "<td>" << histogram.sum() << "</td>";
        oss << "</tr>";
      }
    }
  }
  oss << "</tbody></table>";
  oss << "</td></tr>";

  return oss.str();
}

} // end length_histogram
