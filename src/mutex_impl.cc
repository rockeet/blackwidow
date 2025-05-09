//  Copyright (c) 2017-present, Qihoo, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.


#include <condition_variable>
#include <memory>

#include "src/mutex.h"
#include "src/mutex_impl.h"

namespace blackwidow {

class MutexImpl : public Mutex {
 public:
  MutexImpl() {}
  ~MutexImpl() {}

  Status Lock() override;

  Status TryLockFor(int64_t timeout_time) override;

  void UnLock() override { mutex_.unlock(); }

  friend class CondVarImpl;

 private:
  std::mutex mutex_;
};

class CondVarImpl : public CondVar {
 public:
  CondVarImpl() {}
  ~CondVarImpl() {}

  Status Wait(const std::shared_ptr<Mutex>& mutex) override;

  Status WaitFor(const std::shared_ptr<Mutex>& mutex,
                 int64_t timeout_time) override;

  void Notify() override { cv_.notify_one(); }

  void NotifyAll() override { cv_.notify_all(); }

 private:
  std::condition_variable cv_;
};

std::shared_ptr<Mutex>
MutexFactoryImpl::AllocateMutex() {
  return std::shared_ptr<Mutex>(new MutexImpl());
}

std::shared_ptr<CondVar>
MutexFactoryImpl::AllocateCondVar() {
  return std::shared_ptr<CondVar>(new CondVarImpl());
}

Status MutexImpl::Lock() {
  mutex_.lock();
  return Status::OK();
}

Status MutexImpl::TryLockFor(int64_t timeout_time) {
  bool locked = true;

  if (timeout_time == 0) {
    locked = mutex_.try_lock();
  } else {
    // Previously, this code used a std::timed_mutex.  However, this was changed
    // due to known bugs in gcc versions < 4.9.
    // https://gcc.gnu.org/bugzilla/show_bug.cgi?id=54562
    //
    // Since this mutex isn't held for long and only a single mutex is ever
    // held at a time, it is reasonable to ignore the lock timeout_time here
    // and only check it when waiting on the condition_variable.
    mutex_.lock();
  }

  if (!locked) {
    // timeout acquiring mutex
    return Status::TimedOut(Status::SubCode::kMutexTimeout);
  }

  return Status::OK();
}

Status CondVarImpl::Wait(
    const std::shared_ptr<Mutex>& mutex) {
  auto mutex_impl = reinterpret_cast<MutexImpl*>(mutex.get());

  std::unique_lock<std::mutex> lock(mutex_impl->mutex_, std::adopt_lock);
  cv_.wait(lock);

  // Make sure unique_lock doesn't unlock mutex when it destructs
  lock.release();

  return Status::OK();
}

Status CondVarImpl::WaitFor(
    const std::shared_ptr<Mutex>& mutex, int64_t timeout_time) {
  Status s;

  auto mutex_impl = reinterpret_cast<MutexImpl*>(mutex.get());
  std::unique_lock<std::mutex> lock(mutex_impl->mutex_, std::adopt_lock);

  if (timeout_time < 0) {
    // If timeout is negative, do not use a timeout
    cv_.wait(lock);
  } else {
    auto duration = std::chrono::microseconds(timeout_time);
    auto cv_status = cv_.wait_for(lock, duration);

    // Check if the wait stopped due to timing out.
    if (cv_status == std::cv_status::timeout) {
      s = Status::TimedOut(Status::SubCode::kMutexTimeout);
    }
  }

  // Make sure unique_lock doesn't unlock mutex when it destructs
  lock.release();

  // CV was signaled, or we spuriously woke up (but didn't time out)
  return s;
}

}  // namespace blackwidow
