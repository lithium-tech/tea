#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestError : public FilterTestBase {};

TEST_F(FilterTestError, DivisionOverflow) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{-2147483648});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{-1});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1, col2", "col1 / col2 != 0", ExpectedValues().SetIsError(true));
}

TEST_F(FilterTestError, DivisionByZero) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{123});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{0});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1, col2", "col1 / col2 != 0", ExpectedValues().SetIsError(true));
}

TEST_F(FilterTestError, Overflow) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{65536});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{65536});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1, col2", "col1 * col2 != 0", ExpectedValues().SetIsError(true));
}

}  // namespace
}  // namespace tea
