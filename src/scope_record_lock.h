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
  ScopeRecordLock(LockMgr* lock_mgr, const Slice& key) :
    lock_mgr_(lock_mgr), key_(key) {
    lock_mgr_->TryLock(key_);
  }
  ~ScopeRecordLock() {
    lock_mgr_->UnLock(key_);
  }

 private:
  LockMgr* const lock_mgr_;
  Slice key_;
  ScopeRecordLock(const ScopeRecordLock&);
  void operator=(const ScopeRecordLock&);
};

class MultiScopeRecordLock {
 public:
  MultiScopeRecordLock(LockMgr* lock_mgr, Slice* keys, size_t num) :
      lock_mgr_(lock_mgr), keys_(keys) {
    std::sort(keys, keys + num);
    num_ = num = std::unique(keys, keys + num) - keys;
    for (size_t i = 0; i < num; ++i) {
      lock_mgr->TryLock(keys[i]);
    }
  }
  ~MultiScopeRecordLock() {
    for (size_t i = num_; i; ) {
      i--;
      lock_mgr_->UnLock(keys_[i]);
    }
  }

 private:
  LockMgr* const lock_mgr_;
  Slice* keys_;
  size_t num_;
  MultiScopeRecordLock(const MultiScopeRecordLock&);
  void operator=(const MultiScopeRecordLock&);
};

}  // namespace blackwidow
#endif  // SRC_SCOPE_RECORD_LOCK_H_
