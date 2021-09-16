#include "scope_record_lock.h"

namespace blackwidow {

using rocksdb::Slice;

MultiScopeRecordLock::MultiScopeRecordLock(LockMgr* lm, Slice* keys, size_t num)
    : lock_mgr_(lm), keys_(keys)
{
    std::sort(keys, keys + num);
    num_ = num = std::unique(keys, keys + num) - keys;
    for (size_t i = 0; i < num; ++i) {
        lm->TryLock(keys[i]);
    }
}

MultiScopeRecordLock::~MultiScopeRecordLock() {
    for (size_t i = num_; i;) {
        i--;
        lock_mgr_->UnLock(keys_[i]);
    }
}

} // namespace blackwidow
