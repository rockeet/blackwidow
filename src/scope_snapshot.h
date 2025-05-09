//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SCOPE_SNAPSHOT_H_
#define SRC_SCOPE_SNAPSHOT_H_

#include "rocksdb/db.h"

namespace blackwidow {
class ScopeSnapshot {
 public:
  ScopeSnapshot(rocksdb::DB* db, const rocksdb::Snapshot** snapshot);
  ~ScopeSnapshot();
 private:
  rocksdb::DB* db_;
  const rocksdb::Snapshot** snapshot_;
  ScopeSnapshot(const ScopeSnapshot&) = delete;
  void operator=(const ScopeSnapshot&) = delete;
};

struct ReadOptionsAutoSnapshot : rocksdb::ReadOptions {
  explicit ReadOptionsAutoSnapshot(rocksdb::DB* db);
  ~ReadOptionsAutoSnapshot();
private:
  rocksdb::DB* db_;
};

}  // namespace blackwidow
#endif  // SRC_SCOPE_SNAPSHOT_H_
