#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class NoColumnTest : public TeaTest {};

TEST_F(NoColumnTest, Simple) {
  {
    auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{4, 5, 6});
    auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
    ASSIGN_OR_FAIL(auto data2_path, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data2_path}));
  }

  {
    auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 8});
    ASSIGN_OR_FAIL(auto data1_path, state_->WriteFile({column1}));
    ASSERT_OK(state_->AddDataFiles({data1_path}));
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(
      pq::ScanResult result,
      pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 + col1 >= 7 OR col2 is not null").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"8"}, {"4"}, {"5"}, {"6"}});
  EXPECT_EQ(result, expected);

  std::vector<std::string> filters = stats_state_->GetAllGandivaFilters();
  EXPECT_EQ(filters, (std::vector<std::string>{"bool greater_than_or_equal_to(int32 AddOverflow((int32) col1, (int32) "
                                               "col1), (const int32) 7) || bool not(bool isnull((const int32) null))",
                                               "bool greater_than_or_equal_to(int32 AddOverflow((int32) col1, (int32) "
                                               "col1), (const int32) 7) || bool not(bool isnull((int32) col2))"}));
}

}  // namespace
}  // namespace tea
