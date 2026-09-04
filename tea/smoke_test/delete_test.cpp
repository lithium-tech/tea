#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class DeleteTest : public TeaTest {
 public:
  IFileWriter::Hints FromRgSizes(std::vector<size_t> rg_sizes) {
    return IFileWriter::Hints{.row_group_sizes = std::move(rg_sizes)};
  }
};

TEST_F(DeleteTest, PositionalAndEquality) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2, 6, 8});
  ASSIGN_OR_FAIL(auto pos_del_path, state_->WriteFile({column3, column4}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({pos_del_path}));

  auto column5 = MakeInt64Column("col5", 2, OptionalVector<int64_t>{2});
  ASSIGN_OR_FAIL(auto eq_del_path, state_->WriteFile({column5}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({pos_del_path}, {2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"1"}, {"4"}, {"6"}});

  EXPECT_EQ(result, expected);
}

}  // namespace
}  // namespace tea
