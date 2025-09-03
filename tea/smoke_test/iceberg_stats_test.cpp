#include <string>

#include "gtest/gtest.h"
#include "pg_config.h"  // NOLINT build/include_subdir

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/spark_generated_test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

#ifdef TEA_BUILD_STATS
// PROJECT_DIR/test/iceberg/gen/gperov_test.py
TEST_F(OtherEngineGeneratedTable, Stats) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(int oid, pq::GetTableOid(*conn_, kDefaultTableName));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery("tea_get_stats_from_iceberg(" + std::to_string(oid) +
                                                 ", 'tea://iceberg://gperov.test')")
                                  .Run(*conn_));

  auto expected = pq::ScanResult({"starelid", "staattnum", "stanullfrac", "stawidth", "stadistinct"},
                                 {{std::to_string(oid), "1", "0", "1", ""}, {std::to_string(oid), "2", "0", "1", ""}});
  EXPECT_EQ(result, expected);
}

TEST_F(OtherEngineGeneratedTable, TeapotLocation) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    GTEST_SKIP() << "Skip test with iceberg on external table";
  }
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(int oid, pq::GetTableOid(*conn_, kDefaultTableName));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery("tea_get_stats_from_iceberg(" + std::to_string(oid) +
                                                 ", 'tea://teapot://gperov.test')")
                                  .Run(*conn_));

  auto expected = pq::ScanResult({"starelid", "staattnum", "stanullfrac", "stawidth", "stadistinct"},
                                 {{std::to_string(oid), "1", "0", "1", ""}, {std::to_string(oid), "2", "0", "1", ""}});
  EXPECT_EQ(result, expected);
}
#endif

}  // namespace
}  // namespace tea
