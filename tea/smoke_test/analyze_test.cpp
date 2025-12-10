#include <string>

#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/spark_generated_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

arrow::Status Analyze(pq::PGconnWrapper& conn, const std::string& test_table_name = "test_table") {
  return pq::Command("ANALYZE " + test_table_name).Run(conn);
}

arrow::Status AnalyzeColumn(pq::PGconnWrapper& conn, const std::string& column_name,
                            const std::string& test_table_name = "test_table") {
  return pq::Command("ANALYZE " + test_table_name + " (" + column_name + ")").Run(conn);
}

arrow::Result<pq::ScanResult> GetTableStats(pq::PGconnWrapper& conn,
                                            const std::string& test_table_name = "test_table") {
  return pq::Query("SELECT relpages, reltuples FROM pg_class WHERE relname = '" + test_table_name + "'").Run(conn);
}

arrow::Result<pq::ScanResult> GetColumnsStats(pq::PGconnWrapper& conn,
                                              const std::string& test_table_name = "test_table") {
  return pq::Query(
             "SELECT attname, null_frac, avg_width, n_distinct, most_common_vals, most_common_freqs, "
             "histogram_bounds FROM pg_stats WHERE tablename = '" +
             test_table_name + "'")
      .Run(conn);
}

class AnalyzeTest : public TeaTest {
 protected:
  void GenerateData() {
    int64_t some_value = 0;
    for (int i = 0; i < kFiles; ++i) {
      OptionalVector<int32_t> col_1_data;  // distinct cardinality
      OptionalVector<int32_t> col_2_data;  // low cardinality (uniform)
      OptionalVector<int32_t> col_3_data;  // high cardinality with some popular values
      for (int j = 0; j < kRowsInFile; ++j) {
        col_1_data.emplace_back(some_value++);
        col_2_data.emplace_back(j % 2);
        col_3_data.emplace_back(kRowsInFile / (j + 1));
      }
      auto column1 = MakeInt32Column("col1", 1, std::move(col_1_data));
      auto column2 = MakeInt32Column("col2", 2, std::move(col_2_data));
      auto column3 = MakeInt32Column("col3", 3, std::move(col_3_data));
      ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
      ASSERT_OK(state_->AddDataFiles({data_path}));
    }
  }

  static constexpr int32_t kFiles = 10;
  static constexpr int32_t kRowsInFile = 5'000;

  static constexpr int32_t kAttnameId = 0;
  static constexpr int32_t kNDistinctId = 3;

  // TODO(gmusya): test other
  // constexpr int32_t kNullFracId = 1;
  // constexpr int32_t kAvgWidthId = 2;
  // constexpr int32_t kMostCommonValsId = 4;
  // constexpr int32_t kMostCommonFreqsId = 5;
  // constexpr int32_t kHistogramBounds = 6;
};

TEST_F(AnalyzeTest, AllColumns) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    GTEST_SKIP() << "Skip test with analyze on foreign table";
  }

  GenerateData();

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int4"}}));

  ASSERT_OK(Analyze(*conn_));

  ASSIGN_OR_FAIL(auto pg_class_stats, GetTableStats(*conn_));
  ASSERT_EQ(pg_class_stats.values.size(), 1);
  ASSERT_EQ(pg_class_stats.values[0].size(), 2);
  std::string relpages_str = pg_class_stats.values[0][0];
  std::string reltuples_str = pg_class_stats.values[0][1];
  ASSERT_GT(relpages_str.size(), 0);
  ASSERT_GT(reltuples_str.size(), 0);
  double relpages = std::stold(relpages_str);
  double reltuples = std::stold(reltuples_str);
  EXPECT_EQ(reltuples, kFiles * kRowsInFile);
  EXPECT_GE(relpages, 1);

  ASSIGN_OR_FAIL(auto pg_column_stats, GetColumnsStats(*conn_));
  auto& values = pg_column_stats.values;
  ASSERT_EQ(values.size(), 3);

  std::sort(values.begin(), values.end(),
            [](const auto& lhs, const auto& rhs) { return lhs[kAttnameId] < rhs[kAttnameId]; });

  const auto& col1_info = values[0];
  const auto& col2_info = values[1];
  const auto& col3_info = values[2];

  EXPECT_NEAR(std::stold(col1_info[kNDistinctId]), -1, 1e-6);
  EXPECT_NEAR(std::stold(col2_info[kNDistinctId]), 2, 1e-6);
  EXPECT_NEAR(std::stold(col3_info[kNDistinctId]), 140, 20);

  auto stats = stats_state_->GetStats(true);
  ASSERT_GE(stats.size(), 1);
  EXPECT_TRUE(
      std::any_of(stats.begin(), stats.end(), [](const auto& x) { return x.projection().columns_read() == 3; }));
}

TEST_F(AnalyzeTest, OneColumn) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    GTEST_SKIP() << "Skip test with analyze on foreign table";
  }

  GenerateData();

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int4"}}));

  ASSERT_OK(AnalyzeColumn(*conn_, "col2"));

  ASSIGN_OR_FAIL(auto pg_class_stats, GetTableStats(*conn_));
  ASSERT_EQ(pg_class_stats.values.size(), 1);
  ASSERT_EQ(pg_class_stats.values[0].size(), 2);
  std::string relpages_str = pg_class_stats.values[0][0];
  std::string reltuples_str = pg_class_stats.values[0][1];
  ASSERT_GT(relpages_str.size(), 0);
  ASSERT_GT(reltuples_str.size(), 0);
  double relpages = std::stold(relpages_str);
  double reltuples = std::stold(reltuples_str);
  EXPECT_EQ(reltuples, kFiles * kRowsInFile);
  EXPECT_GE(relpages, 1);

  ASSIGN_OR_FAIL(auto pg_column_stats, GetColumnsStats(*conn_));
  auto& values = pg_column_stats.values;

  std::sort(values.begin(), values.end(),
            [](const auto& lhs, const auto& rhs) { return lhs[kAttnameId] < rhs[kAttnameId]; });

  // stats for col1 and col3 are still unknown
  ASSERT_EQ(values.size(), 1);

  const auto& col2_info = values[0];
  EXPECT_EQ(col2_info[kAttnameId], std::string("col2"));

  EXPECT_NEAR(std::stold(col2_info[kNDistinctId]), 2, 1e-6);
  auto stats = stats_state_->GetStats(true);
  ASSERT_GE(stats.size(), 1);
// TODO(gmusya): read only required columns
#if 0
  EXPECT_TRUE(
       std::any_of(stats.begin(), stats.end(), [](const auto& x) { return x.projection().columns_read() == 1; }));
#endif
}

TEST_F(OtherEngineGeneratedTable, Analyze) {
  if (Environment::GetTableType() != TestTableType::kForeign) {
    GTEST_SKIP();
  }
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSERT_OK(Analyze(*conn_));

  ASSIGN_OR_FAIL(auto pg_class_stats, GetTableStats(*conn_));
  ASSERT_EQ(pg_class_stats.values.size(), 1);
  ASSERT_EQ(pg_class_stats.values[0].size(), 2);
  std::string relpages_str = pg_class_stats.values[0][0];
  std::string reltuples_str = pg_class_stats.values[0][1];
  ASSERT_GT(relpages_str.size(), 0);
  ASSERT_GT(reltuples_str.size(), 0);
  double relpages = std::stold(relpages_str);
  double reltuples = std::stold(reltuples_str);

  EXPECT_LE(9999, reltuples);
  EXPECT_LE(reltuples, 10000);
  EXPECT_GE(relpages, 1);
}

class ExplainTest : public TeaTest {};

TEST_F(ExplainTest, Trivial) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    GTEST_SKIP() << "Skip test with explain on foreign table";
  }
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(auto result, pq::QueryOrCommand("EXPLAIN VERBOSE SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto scan_result = result.first.ToScanResult();
  std::string text;
  for (const auto& row : scan_result.values) {
    ASSERT_EQ(row.size(), 1);
    text += row[0] + "\n";
  }
  std::string schema =
      "\"schema\":{\"id\":0,\"type\":[{\"id\":1,\"is_required\":false,\"name\":\"col1\",\"type\":\"int\"},"
      "{\"id\":2,\"is_required\":false,\"name\":\"col2\",\"type\":\"int\"}]}";
  ASSERT_TRUE(text.find(schema) != std::string::npos) << text;
  ASSERT_TRUE(text.find("Foreign Scan on public.test_table") != std::string::npos) << text;
}

TEST_F(ExplainTest, Filter) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    GTEST_SKIP() << "Skip test with explain on foreign table";
  }
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(auto result,
                 pq::QueryOrCommand("EXPLAIN SELECT * FROM " + kDefaultTableName + " WHERE col1 > 3").Run(*conn_));
  auto scan_result = result.first.ToScanResult();
  std::string text;
  for (const auto& row : scan_result.values) {
    ASSERT_EQ(row.size(), 1);
    text += row[0] + "\n";
  }
  std::string filter_string = "Filter:";
  ASSERT_TRUE(text.find(filter_string) == std::string::npos) << text;
}

}  // namespace
}  // namespace tea
