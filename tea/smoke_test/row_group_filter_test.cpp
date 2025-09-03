#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class RowGroupFilterTest : public TeaTest {};

TEST_F(RowGroupFilterTest, WithFilter) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, std::nullopt, 1, 3, 7});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, std::nullopt, 2, 7});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{1, 1, 3}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "count(*)").SetWhere("col1 >= 2").Run(*conn_));
  auto expected = pq::ScanResult({"count"}, {{"2"}});
  ASSERT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);
  int32_t skipped_row_groups = 0;
  for (const auto& stat : stats) {
    skipped_row_groups += stat.data().row_groups_skipped_filter();
  }

  EXPECT_GE(skipped_row_groups, 1);
}

}  // namespace
}  // namespace tea
