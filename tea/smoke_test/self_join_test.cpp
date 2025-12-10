#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class SelfJoinTest : public TeaTest {};

TEST_F(SelfJoinTest, Simple) {
  constexpr int32_t kFiles = 11;
  constexpr int32_t kRowsInFile = 10000;

  constexpr int32_t kTotalRows = kFiles * kRowsInFile;

  for (int32_t join_count : {1, 2, 3, 5, 10}) {
    std::string table_name = "test_table_joins" + std::to_string(join_count);
    std::vector<std::vector<std::string>> expected_result;

    for (int32_t i = 0; i < kFiles; ++i) {
      OptionalVector<int32_t> col1(kRowsInFile);
      OptionalVector<int32_t> col2(kRowsInFile);

      for (int j = 0; j < kRowsInFile; ++j) {
        col1[j] = i * kRowsInFile + j;
        col2[j] = (col1[j].value() + 1) % kTotalRows;

        expected_result.emplace_back(std::vector<std::string>{
            std::to_string(col1[j].value()), std::to_string((col1[j].value() + join_count + 1) % kTotalRows)});
      }

      auto column1 = MakeInt32Column("col1", 1, col1);
      auto column2 = MakeInt32Column("col2", 2, col2);
      ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
      ASSERT_OK(state_->AddDataFiles({file_path}, table_name));
    }

    ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                    GreenplumColumnInfo{.name = "col2", .type = "int4"}},
                                                   table_name));

    std::string query =
        "SELECT a.col1 AS col1, b.col2 AS col2 FROM " + table_name + " a JOIN " + table_name + " b ON a.col2 = b.col1";

    for (int t = 0; t + 1 < join_count; ++t) {
      query =
          "SELECT a.col1 AS col1, b.col2 AS col2 FROM (" + query + ") a JOIN " + table_name + " b ON a.col2 = b.col1";
    }

    ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query(query).Run(*conn_));

    ASSERT_EQ(expected_result.size(), result.values.size());

    auto expected = pq::ScanResult(result.headers, expected_result);
    ASSERT_EQ(result, expected);
  }
}

TEST_F(SelfJoinTest, CreateTableWithLimit) {
  constexpr int32_t kFiles = 1;
  constexpr int32_t kRowsInFile = 10000;

  constexpr int32_t kTotalRows = kFiles * kRowsInFile;

  for (int32_t join_count : {1, 2, 3, 5, 10}) {
    std::string table_name = "test_table_joins" + std::to_string(join_count);
    std::string table_to_create = "result_table" + std::to_string(join_count);
    std::vector<std::vector<std::string>> expected_result;

    for (int32_t i = 0; i < kFiles; ++i) {
      OptionalVector<int32_t> col1(kRowsInFile);
      OptionalVector<int32_t> col2(kRowsInFile);

      for (int j = 0; j < kRowsInFile; ++j) {
        col1[j] = i * kRowsInFile + j;
        col2[j] = (col1[j].value() + 1) % kTotalRows;

        expected_result.emplace_back(std::vector<std::string>{
            std::to_string(col1[j].value()), std::to_string((col1[j].value() + join_count + 1) % kTotalRows)});
      }

      auto column1 = MakeInt32Column("col1", 1, col1);
      auto column2 = MakeInt32Column("col2", 2, col2);
      ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
      ASSERT_OK(state_->AddDataFiles({file_path}, table_name));
    }

    ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                    GreenplumColumnInfo{.name = "col2", .type = "int4"}},
                                                   table_name));

    std::string query =
        "SELECT a.col1 AS col1, b.col2 AS col2 FROM " + table_name + " a JOIN " + table_name + " b ON a.col2 = b.col1";

    for (int t = 0; t + 1 < join_count; ++t) {
      query = "SELECT * FROM (SELECT a.col1 AS col1, b.col2 AS col2 FROM (" + query + ") a JOIN " + table_name +
              " b ON a.col2 = b.col1 LIMIT 10) t" + std::to_string(t);
    }

    ASSIGN_OR_FAIL(auto _, pq::Query("DROP TABLE IF EXISTS " + table_to_create).Run(*conn_));

    query = "CREATE TABLE " + table_to_create + " as (" + query + ")";
    ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query(query).Run(*conn_));
    ASSIGN_OR_FAIL(auto _2, pq::Query("DROP TABLE IF EXISTS " + table_to_create).Run(*conn_));
  }
}

TEST_F(SelfJoinTest, SimpleWithFilter) {
  constexpr int32_t kFiles = 11;
  constexpr int32_t kRowsInFile = 10000;

  constexpr int32_t kTotalRows = kFiles * kRowsInFile;

  int32_t table_id = 0;
  for (int32_t join_count : {1, 5, 10}) {
    for (const auto& [lower_bound, upper_bound] : std::vector<std::pair<int, int>>{{25000, 26000}, {123, 123}}) {
      std::string table_name = "t" + std::to_string(table_id++);
      std::vector<std::vector<std::string>> expected_result;

      for (int32_t i = 0; i < kFiles; ++i) {
        OptionalVector<int32_t> col1(kRowsInFile);
        OptionalVector<int32_t> col2(kRowsInFile);

        for (int j = 0; j < kRowsInFile; ++j) {
          col1[j] = i * kRowsInFile + j;
          col2[j] = (col1[j].value() + 1) % kTotalRows;

          if (lower_bound <= col1[j].value() && col1[j].value() <= upper_bound)
            expected_result.emplace_back(std::vector<std::string>{
                std::to_string(col1[j].value()), std::to_string((col1[j].value() + join_count + 1) % kTotalRows)});
        }

        auto column1 = MakeInt32Column("col1", 1, col1);
        auto column2 = MakeInt32Column("col2", 2, col2);
        ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
        ASSERT_OK(state_->AddDataFiles({file_path}, table_name));
      }

      ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                      GreenplumColumnInfo{.name = "col2", .type = "int4"}},
                                                     table_name));

      std::string query = "SELECT a.col1 AS col1, b.col2 AS col2 FROM " + table_name + " a JOIN " + table_name +
                          " b ON a.col2 = b.col1 WHERE " + std::to_string(lower_bound) +
                          " <= a.col1 AND a.col1 <= " + std::to_string(upper_bound);

      for (int t = 0; t + 1 < join_count; ++t) {
        query =
            "SELECT a.col1 AS col1, b.col2 AS col2 FROM (" + query + ") a JOIN " + table_name + " b ON a.col2 = b.col1";
      }

      query = "SELECT * FROM (" + query + ") a WHERE " + std::to_string(lower_bound) +
              " <= a.col1 AND a.col1 <= " + std::to_string(upper_bound);

      ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query(query).Run(*conn_));

      ASSERT_EQ(expected_result.size(), result.values.size());

      auto expected = pq::ScanResult(result.headers, expected_result);
      ASSERT_EQ(result, expected);
    }
  }
}

}  // namespace
}  // namespace tea
