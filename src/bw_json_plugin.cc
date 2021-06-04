#include "rocksdb/env.h"
#include "rocksdb/comparator.h"
#include "rocksdb/compaction_filter.h"
#include "redis.h"
#include "custom_comparator.h"
#include "base_filter.h"
#include "lists_filter.h"
#include "zsets_filter.h"
#include "strings_filter.h"
#include <mutex>

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
  std::string m_type;
  std::mutex m_mtx;
  const JsonPluginRepo* m_repo;
  Tpl_FilterFactoryJS(const json& js, const JsonPluginRepo& repo) {
    m_repo = &repo;
    m_type = js["type"];
  }
  virtual std::unique_ptr<rocksdb::CompactionFilter>
  CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context)
  final {
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      if (!this->db_ptr_) {
        DB_MultiCF* dbm = (*m_repo)[m_type];
        ROCKSDB_VERIFY_F(nullptr != dbm, "type = %s", m_type.c_str());
        this->db_ptr_ = &dbm->db;
        this->cf_handles_ptr_ = &dbm->cf_handles;
      }
    }
    return Base::CreateCompactionFilter(context);
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
