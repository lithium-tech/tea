#include <string>

#include "gtest/gtest.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class CountTest : public TeaTest {};

TEST_F(CountTest, Trivial) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(*)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"3"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, NullNull) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3, std::nullopt});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6, std::nullopt});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(*)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"4"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, WithEqualityDelete) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column_del1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{213, 7});
  auto column_del2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{5, 7});
  ASSIGN_OR_FAIL(auto delete_path, state_->WriteFile({column_del1, column_del2}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({delete_path}, {2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(*)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"2"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, WithPositionalDelete) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column5 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  auto column6 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 2});
  ASSIGN_OR_FAIL(auto delete_path, state_->WriteFile({column5, column6}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({delete_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(*)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"1"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, CountColumn) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 7});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(col1)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"2"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, CountExpression) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, std::nullopt});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(col1 + col2)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"1"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, CountDistinctColumn) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, std::nullopt});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(col1 + col2)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"1"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, CountDistinctExpression) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, std::nullopt, 1, 3, 7});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, std::nullopt, 2, 7});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(col1 + col2)").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"3"}});
  ASSERT_EQ(result, expected);
}

TEST_F(CountTest, WithFilter) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, std::nullopt, 1, 3, 7});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, std::nullopt, 2, 7});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "count(*)").SetWhere("col2 >= 3").Run(*this->conn_));
  auto expected = pq::ScanResult({"count"}, {{"3"}});
  ASSERT_EQ(result, expected);
}

}  // namespace
}  // namespace tea
