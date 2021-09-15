//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_SCOPE_RECORD_LOCK_H_
#define SRC_SCOPE_RECORD_LOCK_H_

#include <vector>
#include <string>
#include <algorithm>

#include "lock_mgr.h"

namespace blackwidow {
class ScopeRecordLock {
 public:
  ScopeRecordLock(LockMgr* lock_mgr, const rocksdb::Slice& key) :
    lock_mgr_(lock_mgr), key_(key) {
    lock_mgr_->TryLock(key_);
  }
  ~ScopeRecordLock() {
    lock_mgr_->UnLock(key_);
  }

 private:
  LockMgr* const lock_mgr_;
  rocksdb::Slice key_;
  ScopeRecordLock(const ScopeRecordLock&) = delete;
  void operator=(const ScopeRecordLock&) = delete;
};

class MultiScopeRecordLock {
 public:
  MultiScopeRecordLock(LockMgr* lock_mgr, rocksdb::Slice* keys, size_t num);
  ~MultiScopeRecordLock();

 private:
  LockMgr* const lock_mgr_;
  rocksdb::Slice* keys_;
  size_t num_;
  MultiScopeRecordLock(const MultiScopeRecordLock&) = delete;
  void operator=(const MultiScopeRecordLock&) = delete;
};

}  // namespace blackwidow
#endif  // SRC_SCOPE_RECORD_LOCK_H_
