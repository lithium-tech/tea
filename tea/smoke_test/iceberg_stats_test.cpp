#include <string>

#include "gtest/gtest.h"
#include "pg_config.h"  // NOLINT build/include_subdir

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/spark_generated_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/location.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class MetricsTest : public TeaTest {};

TEST_F(MetricsTest, Simple) {
  ASSIGN_OR_FAIL(auto result,
                 pq::TableScanQuery("iceberg_tables_metrics").SetWhere("location = 'tea://gperov.test'").Run(*conn_));

  auto expected = pq::ScanResult({"location", "total_records", "total_data_files", "total_files_size",
                                  "total_equality_deletes", "total_position_deletes", "total_delete_files"},
                                 {{"tea://gperov.test", "10000", "6", "26597", "0", "1", "1"}});
  EXPECT_EQ(result, expected);
}

TEST_F(MetricsTest, WithProfile) {
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery("iceberg_tables_metrics")
                                  .SetWhere("location = 'tea://gperov.test?profile=samovar'")
                                  .Run(*conn_));

  auto expected = pq::ScanResult({"location", "total_records", "total_data_files", "total_files_size",
                                  "total_equality_deletes", "total_position_deletes", "total_delete_files"},
                                 {{"tea://gperov.test?profile=samovar", "10000", "6", "26597", "0", "1", "1"}});
  EXPECT_EQ(result, expected);
}

TEST_F(MetricsTest, WithAccessType) {
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery("iceberg_tables_metrics")
                                  .SetWhere("location = 'tea://iceberg://gperov.test?profile=samovar'")
                                  .Run(*conn_));

  auto expected =
      pq::ScanResult({"location", "total_records", "total_data_files", "total_files_size", "total_equality_deletes",
                      "total_position_deletes", "total_delete_files"},
                     {{"tea://iceberg://gperov.test?profile=samovar", "10000", "6", "26597", "0", "1", "1"}});
  EXPECT_EQ(result, expected);
}

TEST_F(MetricsTest, SpecialTable) {
  auto maybe_result =
      pq::TableScanQuery("iceberg_tables_metrics")
          .SetWhere("location = 'tea://special://iceberg_tables_metrics?profile=tea_iceberg_get_metrics'")
          .Run(*conn_);

  ASSERT_FALSE(maybe_result.ok());

  ASSERT_TRUE(maybe_result.status().message().find(
                  "Invalid Iceberg table ID iceberg_tables_metrics, expected at least one '.' delimeter") !=
              std::string::npos)
      << maybe_result.status().message();
}

TEST_F(OtherEngineGeneratedTable, Metrics) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery("tea_iceberg_get_metrics('tea://gperov.test')").Run(*conn_));

  auto expected = pq::ScanResult({"total_records", "total_data_files", "total_files_size", "total_equality_deletes",
                                  "total_position_deletes", "total_delete_files"},
                                 {{"10000", "6", "26597", "0", "1", "1"}});
  EXPECT_EQ(result, expected);
}

TEST_F(OtherEngineGeneratedTable, ExternalTableUdf) {
  auto ice_loc = SimpleLocation("gperov", "test", Options{});
  auto loc = Location(std::move(ice_loc));
  auto query =
      pq::CreateExternalTableQuery(std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                                                    GreenplumColumnInfo{.name = "b", .type = "int8"}},
                                   kDefaultTableName, loc);
  ASSIGN_OR_FAIL(auto defer, query.Run(*conn_));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery("tea_external_table_location('public', 'test_table')").Run(*conn_));

  auto expected = pq::ScanResult({"tea_external_table_location"}, {{"tea://gperov.test"}});
  EXPECT_EQ(result, expected);
}

TEST_F(OtherEngineGeneratedTable, NonExistingTable) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  auto maybe_result = pq::TableScanQuery("tea_iceberg_get_metrics('tea://i_am_bad.table')").Run(*conn_);
  ASSERT_FALSE(maybe_result.ok());

  ASSERT_TRUE(maybe_result.status().message().find("NoSuchObjectException(message=no db 'i_am_bad'") !=
              std::string::npos)
      << maybe_result.status().message();
}

TEST_F(OtherEngineGeneratedTable, InvalidLocation) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  auto maybe_result = pq::TableScanQuery("tea_iceberg_get_metrics('i-am-not-a-location')").Run(*conn_);
  ASSERT_FALSE(maybe_result.ok());

  ASSERT_TRUE(maybe_result.status().message().find("IcebergMetricsTable: location must start with 'tea://'") !=
              std::string::npos)
      << maybe_result.status().message();
}

TEST_F(OtherEngineGeneratedTable, InvalidArguments) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  auto maybe_result = pq::TableScanQuery("tea_iceberg_get_metrics(23)").Run(*conn_);
  ASSERT_FALSE(maybe_result.ok());

  ASSERT_TRUE(maybe_result.status().message().find("function tea_iceberg_get_metrics(integer) does not exist") !=
              std::string::npos)
      << maybe_result.status().message();
}

TEST_F(OtherEngineGeneratedTable, NullArgument) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  auto maybe_result = pq::TableScanQuery("tea_iceberg_get_metrics(null)").Run(*conn_);
  ASSERT_FALSE(maybe_result.ok());

  ASSERT_TRUE(maybe_result.status().message().find("tea_iceberg_get_metrics: location must be not null") !=
              std::string::npos)
      << maybe_result.status().message();
}

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
