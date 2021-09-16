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
#include <mutex>
#include <condition_variable>

#include "src/mutex.h"
#include "src/murmurhash.h"
#include <terark/fstring.hpp>
#include <terark/gold_hash_map.hpp>
#include <terark/hash_strmap.hpp>
#include <terark/util/function.hpp>

namespace blackwidow {

// gcc-8.4 -O2 bug cause gold_hash_set fail(-O0 is ok)
// we don't use gold_hash_set, use hash_strmap instead
#define LockMgr_USE_GOLD_HASH_SET 0
using namespace terark;

struct LockMapStripe {
  explicit LockMapStripe(const std::shared_ptr<MutexFactory>& factory) {
    // stripe_mutex = factory->AllocateMutex();
    // stripe_cv = factory->AllocateCondVar();
    // assert(stripe_mutex);
    // assert(stripe_cv);
  #if LockMgr_USE_GOLD_HASH_SET
    keys.reserve(128);
  #else
    keys.reserve(128, 2048);
    keys.enable_freelist(1024); // non freelist is buggy now
  #endif
  }

  // Mutex must be held before modifying keys map
  // std::shared_ptr<Mutex> stripe_mutex;
  std::mutex stripe_mutex;

  // Condition Variable per stripe for waiting on a lock
  //std::shared_ptr<CondVar> stripe_cv;
  std::condition_variable stripe_cv;

  // Locked keys
#if LockMgr_USE_GOLD_HASH_SET
  // gcc-8.4 -O2 bug cause gold_hash_set fail(-O0 is ok)
  gold_hash_set<fstring, fstring_func::hash_align, fstring_func::equal_align> keys;
#else
  terark::hash_strmap<> keys;
#endif
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
  std::unique_lock<std::mutex> lock(stripe->stripe_mutex);

  // Acquire lock if we are able to
  Status result = AcquireLocked(stripe, key);
  if (!result.ok()) {
    // If we weren't able to acquire the lock, we will keep retrying
    do {
      stripe->stripe_cv.wait(lock);
      result = AcquireLocked(stripe, key);
    } while (!result.ok());
  }

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
  TERARK_VERIFY_S(stripe->keys.erase(key), "%s", key);
  if (max_num_locks_ > 0) {
    // Maintain lock count if there is a limit on the number of locks.
    assert(lock_map_->lock_cnt.load(std::memory_order_relaxed) > 0);
    lock_map_->lock_cnt--;
  }
#endif
}

void LockMgr::UnLock(const rocksdb::Slice& key) {
  // Lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map_->GetStripe(key);
  assert(lock_map_->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map_->lock_map_stripes_[stripe_num];

  stripe->stripe_mutex.lock();
  UnLockKey(key, stripe);
  stripe->stripe_mutex.unlock();

  // Signal waiting threads to retry locking
  stripe->stripe_cv.notify_all();
}
}  //  namespace blackwidow
