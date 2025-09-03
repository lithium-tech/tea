#include <string>

#include "gtest/gtest.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class SystemColumnTest : public TeaTest {};

TEST_F(SystemColumnTest, Trivial) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto data1_path, state_->WriteFile({column1, column2}));

  auto column3 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3});
  auto column4 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto data2_path, state_->WriteFile({column1, column2}));

  ASSERT_OK(state_->AddDataFiles({data1_path, data2_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, std::vector<std::string>{"ctid", "gp_segment_id"}).Run(*conn_));
  for (size_t i = 0; i < result.values.size(); ++i) {
    for (size_t j = i + 1; j < result.values.size(); ++j) {
      EXPECT_TRUE(result.values[i][0] != result.values[j][0] || result.values[i][1] != result.values[j][1]);
    }
  }
}

}  // namespace
}  // namespace tea
