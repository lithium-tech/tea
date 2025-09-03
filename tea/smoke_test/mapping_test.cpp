#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class MappingTest : public TeaTest {
 public:
  void PrepareData() {
    // iceberg schema taken from first file in TeaTest
    str = "str1";
    {
      auto column1 = MakeStringColumn("Col1", 1, std::vector<std::string*>{&str});
      auto column2 = MakeInt32Column("cOl2", 2, OptionalVector<int32_t>{11});
      auto column3 = MakeInt64Column("coL3", 3, OptionalVector<int64_t>{0});
      ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
      ASSERT_OK(state_->AddDataFiles({data_path}));
    }
    {
      auto column1 = MakeStringColumn("Col2", 1, std::vector<std::string*>{&str});
      auto column2 = MakeInt32Column("cOl3", 2, OptionalVector<int32_t>{22});
      auto column3 = MakeInt64Column("coL1", 3, OptionalVector<int64_t>{33});
      ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
      ASSERT_OK(state_->AddDataFiles({data_path}));
    }
    {
      auto column1 = MakeStringColumn("Col3", 1, std::vector<std::string*>{&str});
      auto column2 = MakeInt32Column("cOl1", 2, OptionalVector<int32_t>{55});
      auto column3 = MakeInt64Column("coL2", 3, OptionalVector<int64_t>{44});
      ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
      ASSERT_OK(state_->AddDataFiles({data_path}));
    }
  }

 protected:
  std::string str;
};

TEST_F(MappingTest, SimpleFieldIds) {
  PrepareData();

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int8"}}));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, "(col1 || col2) AS value").Run(*conn_));

  pq::ScanResult expected_result({"value"}, {{"str111"}, {"str122"}, {"str155"}});
  EXPECT_EQ(result, expected_result);
}

TEST_F(MappingTest, UppercaseColumnNameInGP) {
  PrepareData();

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "\"cOl1\"", .type = "text"},
                                                  GreenplumColumnInfo{.name = "\"coL2\"", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "\"Col3\"", .type = "int8"}}));

  ASSIGN_OR_FAIL(auto result,
                 pq::TableScanQuery(kDefaultTableName, "\"Col3\", (\"cOl1\" || \"coL2\") AS value").Run(*conn_));

  pq::ScanResult expected_result({"Col3", "value"}, {{"0", "str111"}, {"33", "str122"}, {"44", "str155"}});
  EXPECT_EQ(result, expected_result);
}

TEST_F(MappingTest, UppercaseColumnNameInGPWithFilter) {
  PrepareData();

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "\"cOl1\"", .type = "text"},
                                                  GreenplumColumnInfo{.name = "\"coL2\"", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "\"Col3\"", .type = "int8"}}));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, "\"Col3\", (\"cOl1\" || \"coL2\") AS value")
                                  .SetWhere("\"Col3\" + \"coL2\" != 55")
                                  .Run(*conn_));

  pq::ScanResult expected_result({"Col3", "value"}, {{"0", "str111"}, {"44", "str155"}});
  EXPECT_EQ(result, expected_result);
}

}  // namespace
}  // namespace tea
