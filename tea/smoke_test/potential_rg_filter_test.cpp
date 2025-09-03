#include <cstdint>
#include <string>

#include "gtest/gtest.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/common.h"

namespace tea {
namespace {

class Prefilter : public TeaTest {};

TEST_F(Prefilter, Trivial) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{7, 5, 9});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "count(col3)").SetWhere("col1 + col2 < 3").Run(*conn_));
  auto expected = pq::ScanResult({"count"}, {{"0"}});
  ASSERT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);
  int64_t read = 0;
  int64_t filtered = 0;
  int64_t prefiltered = 0;
  for (const auto& stat : stats) {
    read += stat.data().rows_read();
    filtered += stat.data().rows_skipped_filter();
    prefiltered += stat.data().rows_skipped_prefilter();
  }

  EXPECT_EQ(read, 0);
  EXPECT_EQ(filtered, 3);
  EXPECT_EQ(prefiltered, 3);
}

TEST_F(Prefilter, Suffix) {
  auto data1 = OptionalVector<int32_t>{};
  auto data2 = OptionalVector<int32_t>{};
  auto data3 = OptionalVector<int32_t>{};

  constexpr int32_t kRowsTotal = 1'000'000;
  constexpr int32_t kMatchingRows = 10'000;

  for (int i = 0; i < kMatchingRows; ++i) {
    data1.push_back(i % 3);
    data2.push_back(2 - i % 3);
    data3.push_back(i);
  }

  for (int i = kMatchingRows; i < kRowsTotal; ++i) {
    data1.push_back(i % 3);
    data2.push_back(50 - i % 3);
    data3.push_back(i);
  }

  auto column1 = MakeInt32Column("col1", 1, std::move(data1));
  auto column2 = MakeInt32Column("col2", 2, std::move(data2));
  auto column3 = MakeInt32Column("col3", 3, std::move(data3));

  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "count(col3)").SetWhere("col1 + col2 < 3").Run(*conn_));
  auto expected = pq::ScanResult({"count"}, {{"10000"}});
  ASSERT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);
  int64_t read = 0;
  int64_t filtered = 0;
  int64_t prefiltered = 0;
  for (const auto& stat : stats) {
    read += stat.data().rows_read();
    filtered += stat.data().rows_skipped_filter();
    prefiltered += stat.data().rows_skipped_prefilter();
  }

  EXPECT_EQ(read, 10000);
  EXPECT_EQ(filtered, 990'000);
  EXPECT_GE(prefiltered, 900'000);
}

}  // namespace
}  // namespace tea
