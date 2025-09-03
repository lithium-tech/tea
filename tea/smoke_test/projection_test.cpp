#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"

#include "tea/common/config.h"
#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class ProjectionTest : public TeaTest {};

std::vector<std::string> GetUsedColumns(const std::vector<std::string>& columns_to_find,
                                        const std::vector<std::string>& projection, const std::string& filter) {
  std::vector<std::string> result;
  for (const auto& column : columns_to_find) {
    for (const auto& projection_column : projection) {
      if (projection_column.find(column) != std::string::npos) {
        result.emplace_back(column);
      }
    }
    if (filter.find(column) != std::string::npos) {
      result.emplace_back(column);
    }
  }
  std::sort(result.begin(), result.end());
  result.erase(std::unique(result.begin(), result.end()), result.end());
  return result;
}

TEST_F(ProjectionTest, WithCondition) {
  std::string str = "str1";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{123});
  auto column3 = MakeInt64Column("col3", 3, OptionalVector<int64_t>{21});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col0", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int8"}}));

  std::vector<std::vector<std::string>> projections = {
      {"col1"}, {"col2"}, {"col1", "col2"}, {"col3"}, {"col1", "col3"}, {"col2", "col3"}, {"col1", "col2", "col3"}};
  std::vector<std::string> conditions = {"col1 > \'asd\'", "col2 > 5", "col3 > 7", "1 = 1"};
  std::vector<std::string> column_names = {"col1", "col2", "col3"};
  for (auto& projection : projections) {
    for (auto& condition : conditions) {
      std::string projection_str;
      for (size_t i = 0; i < projection.size(); ++i) {
        if (i != 0) {
          projection_str += ", ";
        }
        projection_str += projection[i];
      }
      ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, projection).SetWhere(condition).Run(*conn_));
      for (auto& stat : stats_state_->GetStats(false)) {
        if (stat.data().data_files_read() > 0) {
          EXPECT_EQ(stat.projection().columns_read(), (GetUsedColumns(column_names, projection, condition).size()));
          if (Environment::GetTableType() == TestTableType::kForeign) {
            EXPECT_EQ(stat.projection().columns_for_greenplum(), projection.size())
                << projection_str << "; " << condition;
          } else {
            EXPECT_EQ(stat.projection().columns_for_greenplum(),
                      (GetUsedColumns(column_names, projection, condition).size()))
                << projection_str << "; " << condition;
          }
        }
      }
      EXPECT_EQ(result.values.size(), 1);
    }
  }
}

TEST_F(ProjectionTest, WithEqualityDelete) {
  std::string str = "str1";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{123});
  auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{3.7});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto eq_del_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({data_path}, {1, 2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "float4"}}));

  ASSIGN_OR_FAIL(auto result,
                 pq::TableScanQuery(kDefaultTableName, std::vector<std::string>{"col2", "col3"}).Run(*conn_));

  for (auto& stat : stats_state_->GetStats(false)) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 3);
      EXPECT_EQ(stat.projection().columns_for_greenplum(), 2);
      EXPECT_EQ(stat.projection().columns_equality_delete(), 2);
      EXPECT_EQ(stat.projection().columns_only_for_equality_delete(), 1);
    }
  }

  EXPECT_EQ(result.values.size(), 0);
}

TEST_F(ProjectionTest, WithOperation) {
  std::string str = "str1";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{123});
  auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{3.7});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "float4"}}));

  ASSIGN_OR_FAIL(auto result,
                 pq::TableScanQuery(kDefaultTableName, std::vector<std::string>{"col1", "col3 + col3"}).Run(*conn_));
  EXPECT_EQ(result.values.size(), 1);
  for (auto& stat : stats_state_->GetStats(false)) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 2);
    }
  }
}

TEST_F(ProjectionTest, WithUnaryFilter) {
  std::string str = "str1";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{-123});
  auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{3.7});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "float4"}}));

  ASSIGN_OR_FAIL(
      auto result,
      pq::TableScanQuery(kDefaultTableName, std::vector<std::string>{"col1"}).SetWhere("-col2 > 7").Run(*conn_));
  EXPECT_EQ(result.values.size(), 1);
  for (auto& stat : stats_state_->GetStats(false)) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 2);
    }
  }
}

TEST_F(ProjectionTest, DropColumn) {
  std::string str = "str1";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{123});
  auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{3.7});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "float4"}}));

  ASSERT_OK(pq::DropColumn(*conn_, "test_table", "col2"));

  std::vector<std::string> projections = {"col1", "col3", "col1, col3"};
  std::vector<pq::ScanResult> result_for_projection = {pq::ScanResult({"col1"}, {{"str1"}}),
                                                       pq::ScanResult({"col3"}, {{"3.7"}}),
                                                       pq::ScanResult({"col1", "col3"}, {{"str1", "3.7"}})};
  std::vector<std::string> conditions = {"col1 > \'asd\'", "col3 > 3.6"};
  std::vector<std::string> column_names = {"col1", "col2", "col3"};
  for (size_t i = 0; i < projections.size(); ++i) {
    const auto& expected_result = result_for_projection[i];
    const auto& projection = projections[i];
    for (auto& condition : conditions) {
      ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, projection).SetWhere(condition).Run(*conn_));
      EXPECT_EQ(result, expected_result);
      for (auto& stat : stats_state_->GetStats(false)) {
        if (stat.data().data_files_read() > 0) {
          EXPECT_EQ(stat.projection().columns_read(), (GetUsedColumns(column_names, {projection}, condition).size()));
        }
      }
    }
  }
}

TEST_F(ProjectionTest, SchemaEvolution) {
  // MetadataWriter expects that first file contains all columns
  for (int i = 7; i < 16 + 7; ++i) {
    if ((i & 7) == 0) {
      continue;
    }
    std::string str = "str1";
    auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str, &str, &str});
    auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{123, 10, 140});
    auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{3.7, 1, 7});
    std::vector<ParquetColumn> columns;
    if (i & 1) {
      columns.emplace_back(std::move(column1));
    }
    if (i & 2) {
      columns.emplace_back(std::move(column2));
    }
    if (i & 4) {
      columns.emplace_back(std::move(column3));
    }
    ASSIGN_OR_FAIL(auto data_path, state_->WriteFile(columns));
    ASSERT_OK(state_->AddDataFiles({data_path}));
  }
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "float4"}}));

  std::vector<std::vector<std::string>> projections = {
      {"col1"}, {"col2"}, {"col1", "col2"}, {"col3"}, {"col1", "col3"}, {"col2", "col3"}, {"col1", "col2", "col3"}};
  std::vector<std::string> conditions = {"(col1 || col1) > \'asd\'", "col2 + col2 > 12", "col3 + col3 > 7.2"};
  for (auto& projection : projections) {
    for (auto& condition : conditions) {
      auto query = pq::TableScanQuery(kDefaultTableName, projection).SetWhere(condition);
      ASSIGN_OR_FAIL(auto result, query.Run(*conn_));
    }
  }
}

TEST_F(ProjectionTest, RowNumber) {
  std::string str = "str1";
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3, 4, 5, 6, 7});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 1, 2, 3, 1, 3, 2});
  auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{3.7, 3.5, 3.2, 3.1, 3.4, 3.6, 3.3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "float4"}}));

  std::vector<std::string> projections = {"col1, row_number() over()", "row_number() over(partition by col2)",
                                          "col1, row_number() over(order by col3)",
                                          "col1, row_number() over(partition by col2 order by col3)"};
  std::vector<pq::ScanResult> result_for_projection = {
      pq::ScanResult({"col1", "row_number"},
                     {{"1", "1"}, {"2", "2"}, {"3", "3"}, {"4", "4"}, {"5", "5"}, {"6", "6"}, {"7", "7"}}),
      pq::ScanResult({"row_number"}, {{"1"}, {"2"}, {"1"}, {"1"}, {"3"}, {"2"}, {"2"}}),
      pq::ScanResult({"col1", "row_number"},
                     {{"1", "7"}, {"2", "5"}, {"3", "2"}, {"4", "1"}, {"5", "4"}, {"6", "6"}, {"7", "3"}}),
      pq::ScanResult({"col1", "row_number"},
                     {{"5", "1"}, {"2", "2"}, {"1", "3"}, {"3", "1"}, {"7", "2"}, {"4", "1"}, {"6", "2"}})};
  std::vector<std::string> column_names = {"col1", "col2", "col3"};
  for (size_t i = 0; i < projections.size(); ++i) {
    const auto& expected_result = result_for_projection[i];
    const auto& projection = projections[i];
    ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, projection).Run(*conn_));
    EXPECT_EQ(result, expected_result) << "projection: " << projection;
    for (auto& stat : stats_state_->GetStats(false)) {
      if (stat.data().data_files_read() > 0) {
        EXPECT_EQ(stat.projection().columns_read(), (GetUsedColumns(column_names, {projection}, "").size()));
      }
    }
  }
}

TEST_F(ProjectionTest, IsDistinctFrom) {
  std::string str = "str1";
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3, 4, 5, 6, 7});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 1, 2, 3, 1, 3, 2});
  auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{3.7, 3.5, 3.2, 3.1, 3.4, 3.6, 3.3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "float4"}}));

  ASSIGN_OR_FAIL(auto result,
                 pq::TableScanQuery(kDefaultTableName, "col3").SetWhere("col2 is distinct from 1").Run(*conn_));
  bool check_projection = true;
#if PG_VERSION_MAJOR >= 9
  if (Environment::GetTableType() == TestTableType::kExternal) {
    check_projection = false;
  }
#endif

  if (check_projection) {
    for (auto& stat : stats_state_->GetStats(false)) {
      if (stat.data().data_files_read() > 0) {
        EXPECT_EQ(stat.projection().columns_read(), 2);
      }
    }
  }
}

}  // namespace
}  // namespace tea
