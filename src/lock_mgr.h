//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef SRC_LOCK_MGR_H_
#define SRC_LOCK_MGR_H_

#include <string>
#include <memory>

#include "mutex.h"
#include <rocksdb/slice.h>

namespace blackwidow {

struct LockMap;
struct LockMapStripe;

class LockMgr {
 public:
  LockMgr(size_t default_num_stripes, int64_t max_num_locks,
          const std::shared_ptr<MutexFactory>& factory);

  ~LockMgr();

  // Attempt to lock key.  If OK status is returned, the caller is responsible
  // for calling UnLock() on this key.
  Status TryLock(const rocksdb::Slice& key);

  // Unlock a key locked by TryLock().
  void UnLock(const rocksdb::Slice& key);

 private:
  // Default number of lock map stripes
  const size_t default_num_stripes_;

  // Limit on number of keys locked per column family
  const int64_t max_num_locks_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<MutexFactory> mutex_factory_;

  // Map to locked key info
  std::shared_ptr<LockMap> lock_map_;

  Status Acquire(LockMapStripe* stripe, const rocksdb::Slice& key);

  Status AcquireLocked(LockMapStripe* stripe, const rocksdb::Slice& key);

  void UnLockKey(const rocksdb::Slice& key, LockMapStripe* stripe);

  // No copying allowed
  LockMgr(const LockMgr&) = delete;
  void operator=(const LockMgr&) = delete;
};

}  //  namespace blackwidow
#endif  // SRC_LOCK_MGR_H_
