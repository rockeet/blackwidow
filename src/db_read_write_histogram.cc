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

#include "slash/include/env.h"
#include "db_read_write_histogram.h"

using std::cout;
using std::endl;

namespace db_rw_histogram {

static const rocksdb::HistogramBucketMapper bucketMapper;

void DbReadWriteHistogram::reset() {
  data->check_sum = 0;
  for (int i = 0; i < DBTypeMax; i++) {
    for (int j = 0; j < ProcessTypeMax; j++) {
      for (int k = 0; k < FieldValueMax; k++) { data->HistogramTable[i][j][k].Clear(); }
    }
  }
}
// 返回值校验
// fstat 检查长度
// checksum 校验文件
DbReadWriteHistogram::DbReadWriteHistogram(const std::string &path) {
  bool exist = slash::FileExists(path);
  if (!exist) {
    fd = open(path.c_str(), O_RDWR|O_CREAT);
    ftruncate(fd, sizeof(*data));
  } else {
    fd = open(path.c_str(), O_RDWR);
  }
  if (fd < 0) LOG(FATAL) << "DbReadWriteHistogram error fd:" << fd;

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result != 0) LOG(FATAL) << "DbReadWriteHistogram get file size error:" << errno;
  if (buf.st_size != sizeof(*data)) {
    LOG(FATAL) << "DbReadWriteHistogram file:"<< path << " size error current:" << buf.st_size << "!=" << sizeof(*data);
  }

  auto addr = mmap(NULL, sizeof(*data), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) LOG(FATAL) << "DbReadWriteHistogram mmap failed";
  data = (HistogramData*)addr;

  if (exist && data->check_sum != get_check_sum()) {
    LOG(ERROR) << "DbReadWriteHistogram check sum failed";
  }

  if (!exist) reset();
}

DbReadWriteHistogram::~DbReadWriteHistogram() {
  data->check_sum = get_check_sum();
  munmap((void*)data, sizeof(*data));
  data = nullptr;
  close(fd);
}

long DbReadWriteHistogram::get_check_sum() {
  //待实现具体内容
  return 0;
}

void DbReadWriteHistogram::Add_Histogram_Metric(const data_type type, process_type step, field_value field, long value) {
  assert(type<DBTypeMax);
  assert(step<ProcessTypeMax);
  data->HistogramTable[type][step][field].Add(value);
}

std::string DbReadWriteHistogram::get_metric() {
  std::ostringstream oss;
  for (int type = 0; type < DBTypeMax; type++) {
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
        //oss<<"pika_cost_time_max_bucket{"<<add_label()<<"} "<<std::to_string(bucketMapper.BucketLimit(limit))<<"\n";
      }
    }
  }

  return oss.str();
}

} // end db_rw_histogram