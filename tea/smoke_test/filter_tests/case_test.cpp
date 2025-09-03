#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestCase : public FilterTestBase {};

TEST_F(FilterTestCase, Simple) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{6, 9, 15, 13});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{0, 0, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1, col2", "CASE WHEN col2 > 0 THEN col1 / col2 <= 1 ELSE false END",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"if (bool greater_than((int32) col2, (const int32) 0)) { bool "
                                            "less_than_or_equal_to(int32 DivSafe((int32) col1, "
                                            "(int32) col2), (const int32) 1) } else { (const bool) 0 }"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "CASE WHEN col2 > 0 THEN col1 / col2 <= 1 ELSE col1 <= 7 END",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"if (bool greater_than((int32) col2, (const int32) 0)) { bool "
                                            "less_than_or_equal_to(int32 DivSafe((int32) col1, "
                                            "(int32) col2), (const int32) 1) } else { bool "
                                            "less_than_or_equal_to((int32) col1, (const int32) 7) }"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"6", "0"}, {"13", "15"}})));
}

}  // namespace
}  // namespace tea
