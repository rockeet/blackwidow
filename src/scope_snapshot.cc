// created by leipeng 2021-09-10

#include "scope_snapshot.h"
#include <db/db_impl/db_impl_secondary.h>

namespace blackwidow {

ScopeSnapshot::ScopeSnapshot(rocksdb::DB* db, const rocksdb::Snapshot** snapshot)
    : snapshot_(snapshot)
{
    if (dynamic_cast<rocksdb::DBImplSecondary*>(db)) {
        // DBImplSecondary::NewIterator does not support snapshot
        db_ = nullptr;
        *snapshot_ = nullptr;
    } else {
        db_ = db;
        *snapshot_ = db->GetSnapshot();
    }
}

ScopeSnapshot::~ScopeSnapshot() {
    if (db_)
        db_->ReleaseSnapshot(*snapshot_);
}

ReadOptionsAutoSnapshot::ReadOptionsAutoSnapshot(rocksdb::DB* db) {
    if (dynamic_cast<rocksdb::DBImplSecondary*>(db)) {
        // DBImplSecondary::NewIterator does not support snapshot
        db_ = nullptr;
    } else {
        db_ = db;
        snapshot = db->GetSnapshot();
    }
}

ReadOptionsAutoSnapshot::~ReadOptionsAutoSnapshot() {
    if (db_)
        db_->ReleaseSnapshot(snapshot);
}

} // namespace blackwidow
