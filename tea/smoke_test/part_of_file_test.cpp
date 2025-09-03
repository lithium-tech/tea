#include <string>

#include "gtest/gtest.h"

#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/teapot_test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class PartOfFileTest : public TeapotTest {};

TEST_F(PartOfFileTest, First) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, FromRgSizes({1, 1, 1})));

  auto offsets = GetParquetRowGroupOffsets(data_path);
  SetTeapotResponse({FragmentInfo(data_path).SetPosition(offsets[0]).SetLength(offsets[1] - offsets[0])});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"1"}});
  ASSERT_EQ(result, expected);
}

TEST_F(PartOfFileTest, Second) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, FromRgSizes({1, 1, 1})));

  auto offsets = GetParquetRowGroupOffsets(data_path);
  SetTeapotResponse({FragmentInfo(data_path).SetPosition(offsets[1]).SetLength(offsets[2] - offsets[1])});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}});
  ASSERT_EQ(result, expected);
}

TEST_F(PartOfFileTest, Third) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, FromRgSizes({1, 1, 1})));

  auto offsets = GetParquetRowGroupOffsets(data_path);
  SetTeapotResponse({FragmentInfo(data_path).SetPosition(offsets[2]).SetLength(0)});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"3"}});
  ASSERT_EQ(result, expected);
}

TEST_F(PartOfFileTest, Brute) {
  constexpr int kRowGroups = 5;
  OptionalVector<int32_t> column(kRowGroups);
  for (int i = 0; i < kRowGroups; ++i) {
    column[i] = i;
  }
  auto column1 = MakeInt32Column("col1", 1, column);

  std::vector<size_t> row_group_sizes(kRowGroups, 1);
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, FromRgSizes(row_group_sizes)));

  auto offsets = GetParquetRowGroupOffsets(data_path);

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  std::vector<std::string> data_as_str(kRowGroups);
  for (int i = 0; i < kRowGroups; ++i) {
    data_as_str[i] = std::to_string(i);
  }

  for (int i = 1; i < (1 << kRowGroups); ++i) {
    std::vector<int> to_select;
    for (int j = 0; j < kRowGroups; ++j) {
      if ((1 << j) & i) {
        to_select.push_back(j);
      }
    }

    std::vector<FragmentInfo> fragments;
    for (int rg_num : to_select) {
      fragments.emplace_back(FragmentInfo(data_path)
                                 .SetPosition(offsets[rg_num])
                                 .SetLength(rg_num + 1 == kRowGroups ? 0 : offsets[rg_num + 1] - offsets[rg_num]));
    }

    SetTeapotResponse(fragments);

    std::vector<std::string> expected_headers = {"col1"};
    std::vector<std::vector<std::string>> expected_values;
    for (int rg_num : to_select) {
      expected_values.emplace_back(std::vector<std::string>{data_as_str[rg_num]});
    }

    ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));

    ASSERT_EQ(result, pq::ScanResult(std::move(expected_headers), std::move(expected_values)));
  }
}

TEST_F(PartOfFileTest, MultipleRowGroupsOneFragment) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3, 4, 5});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, FromRgSizes({1, 1, 1, 1, 1})));

  auto offsets = GetParquetRowGroupOffsets(data_path);
  SetTeapotResponse({FragmentInfo(data_path).SetFromTo(offsets[1], offsets[3])});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"3"}});
  ASSERT_EQ(result, expected);
}

TEST_F(PartOfFileTest, FirstByteInRowGroup) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3, 4, 5});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, FromRgSizes({1, 1, 1, 1, 1})));

  auto offsets = GetParquetRowGroupOffsets(data_path);
  SetTeapotResponse({FragmentInfo(data_path).SetFromTo(offsets[1], offsets[3] + 1)});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"3"}, {"4"}});
  ASSERT_EQ(result, expected);
}

TEST_F(PartOfFileTest, WithEmptySegment) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3, 4, 5});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, FromRgSizes({1, 1, 1, 1, 1})));

  auto offsets = GetParquetRowGroupOffsets(data_path);
  SetTeapotResponse({FragmentInfo(data_path).SetFromTo(offsets[1] + 1, offsets[2]),
                     FragmentInfo(data_path).SetFromTo(offsets[3], offsets[3] + 1)});

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"4"}});
  ASSERT_EQ(result, expected);
}

}  // namespace
}  // namespace tea
