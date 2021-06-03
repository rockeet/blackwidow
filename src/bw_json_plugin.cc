#include "rocksdb/env.h"
#include "rocksdb/comparator.h"
#include "rocksdb/compaction_filter.h"
#include "redis.h"
#include "custom_comparator.h"
#include "base_filter.h"
#include "lists_filter.h"
#include "zsets_filter.h"
#include "strings_filter.h"

#undef Debug
#undef Trace
#include "utilities/json/json_plugin_factory.h"

namespace blackwidow {

using namespace rocksdb;

ROCKSDB_REG_DEFAULT_CONS( BaseMetaFilterFactory, CompactionFilterFactory);
ROCKSDB_REG_DEFAULT_CONS(ListsMetaFilterFactory, CompactionFilterFactory);

ROCKSDB_REG_DEFAULT_CONS( StringsFilterFactory, CompactionFilterFactory);

template<class Base>
struct Tpl_FilterFactoryJS : public Base {
  Tpl_FilterFactoryJS(const json& js, const JsonPluginRepo& repo) {
    std::string type = js["type"];
    DB_MultiCF* dbm = repo[type];
    ROCKSDB_VERIFY_F(nullptr != dbm, "type = %s", type.c_str());
    this->db_ptr_ = &dbm->db;
    this->cf_handles_ptr_ = &dbm->cf_handles;
  }
};
using   BaseDataFilterFactoryJS = Tpl_FilterFactoryJS<  BaseDataFilterFactory>;
using  ListsDataFilterFactoryJS = Tpl_FilterFactoryJS< ListsDataFilterFactory>;
using ZSetsScoreFilterFactoryJS = Tpl_FilterFactoryJS<ZSetsScoreFilterFactory>;
ROCKSDB_REG_JSON_REPO_CONS(  "BaseDataFilterFactory",
                              BaseDataFilterFactoryJS, CompactionFilterFactory);
ROCKSDB_REG_JSON_REPO_CONS( "ListsDataFilterFactory",
                             ListsDataFilterFactoryJS, CompactionFilterFactory);
ROCKSDB_REG_JSON_REPO_CONS("ZSetsScoreFilterFactory",
                            ZSetsScoreFilterFactoryJS, CompactionFilterFactory);

const rocksdb::Comparator*  ListsDataKeyComparator();
const rocksdb::Comparator* ZSetsScoreKeyComparator();
static const rocksdb::Comparator*
JS_ListsDataKeyComparator(const json&, const JsonPluginRepo&) {
  return ListsDataKeyComparator();
}
static const rocksdb::Comparator*
JS_ZSetsScoreKeyComparator(const json&, const JsonPluginRepo&) {
  return ZSetsScoreKeyComparator();
}

ROCKSDB_FACTORY_REG("blackwidow.ListsDataKeyComparator",
                             JS_ListsDataKeyComparator);
ROCKSDB_FACTORY_REG("blackwidow.ZSetsScoreKeyComparator",
                             JS_ZSetsScoreKeyComparator);

} // namespace blackwidow
