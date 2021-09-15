//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "lock_mgr.h"

#include <vector>
#include <unordered_set>
#include <atomic>
#include <memory>

#include "src/mutex.h"
#include "src/murmurhash.h"
#include <terark/hash_strmap.hpp>
#include <terark/util/function.hpp>

namespace blackwidow {

struct LockMapStripe {
  explicit LockMapStripe(const std::shared_ptr<MutexFactory>& factory) {
    stripe_mutex = factory->AllocateMutex();
    stripe_cv = factory->AllocateCondVar();
    assert(stripe_mutex);
    assert(stripe_cv);
    keys.reserve(128, 2048);
    keys.enable_freelist(16384); // non freelist is buggy now
  }

  // Mutex must be held before modifying keys map
  std::shared_ptr<Mutex> stripe_mutex;

  // Condition Variable per stripe for waiting on a lock
  std::shared_ptr<CondVar> stripe_cv;

  // Locked keys
  terark::hash_strmap<> keys;
};

// Map of #num_stripes LockMapStripes
struct LockMap {
  explicit LockMap(size_t num_stripes,
                   const std::shared_ptr<MutexFactory>& factory)
      : num_stripes_(num_stripes) {
    lock_map_stripes_.reserve(num_stripes);
    for (size_t i = 0; i < num_stripes; i++) {
      LockMapStripe* stripe = new LockMapStripe(factory);
      lock_map_stripes_.push_back(stripe);
    }
  }

  ~LockMap() {
    for (auto stripe : lock_map_stripes_) {
      delete stripe;
    }
  }

  // Number of sepearate LockMapStripes to create, each with their own Mutex
  const size_t num_stripes_;

  // Count of keys that are currently locked.
  // (Only maintained if LockMgr::max_num_locks_ is positive.)
  std::atomic<int64_t> lock_cnt{0};

  std::vector<LockMapStripe*> lock_map_stripes_;

  size_t GetStripe(const rocksdb::Slice& key) const;
};

size_t LockMap::GetStripe(const rocksdb::Slice& key) const {
  assert(num_stripes_ > 0);
  static murmur_hash hash;
  size_t stripe = hash(key) % num_stripes_;
  return stripe;
}

LockMgr::LockMgr(size_t default_num_stripes,
                 int64_t max_num_locks,
                 const std::shared_ptr<MutexFactory>& mutex_factory)
    : default_num_stripes_(default_num_stripes),
      max_num_locks_(max_num_locks),
      mutex_factory_(mutex_factory),
      lock_map_(std::shared_ptr<LockMap>(
            new LockMap(default_num_stripes, mutex_factory))) {}

LockMgr::~LockMgr() {}

Status LockMgr::TryLock(const rocksdb::Slice& key) {
#ifdef LOCKLESS
  return Status::OK();
#else
  size_t stripe_num = lock_map_->GetStripe(key);
  assert(lock_map_->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map_->lock_map_stripes_[stripe_num];

  return Acquire(stripe, key);
#endif
}

// Helper function for TryLock().
Status LockMgr::Acquire(LockMapStripe* stripe,
                        const rocksdb::Slice& key) {
  // we wait indefinitely to acquire the lock
  Status result = stripe->stripe_mutex->Lock();
#if 1
  TERARK_VERIFY_S(result.ok(), "%s", result.ToString());
#else
  if (!result.ok()) {
    // failed to acquire mutex
    return result;
  }
#endif

  // Acquire lock if we are able to
  result = AcquireLocked(stripe, key);

  if (!result.ok()) {
    // If we weren't able to acquire the lock, we will keep retrying
    do {
      result = stripe->stripe_cv->Wait(stripe->stripe_mutex);
      if (result.ok()) {
        result = AcquireLocked(stripe, key);
      }
    } while (!result.ok());
  }

  stripe->stripe_mutex->UnLock();

  return result;
}

// Try to lock this key after we have acquired the mutex.
// REQUIRED:  Stripe mutex must be held.
Status LockMgr::AcquireLocked(LockMapStripe* stripe,
                              const rocksdb::Slice& key) {
  Status result;
  // Check lock limit
  if (max_num_locks_ > 0 &&
      lock_map_->lock_cnt.load(std::memory_order_relaxed) >= max_num_locks_) {
    if (!stripe->keys.exists(key))
      result = Status::Busy(Status::SubCode::kLockLimit);
    else
      result = Status::Busy(Status::SubCode::kLockTimeout);
  }
  else {
    // Check if this key is already locked
    if (!stripe->keys.insert_i(key).second) { // existed
      // Lock already held
        result = Status::Busy(Status::SubCode::kLockTimeout);
    } else {  // Lock not held.
      // Maintain lock count if there is a limit on the number of locks
      if (max_num_locks_) {
        lock_map_->lock_cnt++;
      }
    }
  }
  return result;
}

void LockMgr::UnLockKey(const rocksdb::Slice& key, LockMapStripe* stripe) {
#ifdef LOCKLESS
#else
  const size_t idx = stripe->keys.find_i(key);
  if (stripe->keys.end_i() != idx) {
    // Found the key locked.  unlock it.
    stripe->keys.erase_i(idx);
    if (max_num_locks_ > 0) {
      // Maintain lock count if there is a limit on the number of locks.
      assert(lock_map_->lock_cnt.load(std::memory_order_relaxed) > 0);
      lock_map_->lock_cnt--;
    }
  } else {
    // This key is either not locked or locked by someone else.
  }
#endif
}

void LockMgr::UnLock(const rocksdb::Slice& key) {
  // Lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map_->GetStripe(key);
  assert(lock_map_->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map_->lock_map_stripes_[stripe_num];

  stripe->stripe_mutex->Lock();
  UnLockKey(key, stripe);
  stripe->stripe_mutex->UnLock();

  // Signal waiting threads to retry locking
  stripe->stripe_cv->NotifyAll();
}
}  //  namespace blackwidow
