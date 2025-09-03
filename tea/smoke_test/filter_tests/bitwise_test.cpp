#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestBitwise : public FilterTestBase {};

TEST_F(FilterTestBitwise, Int4) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter("col1", "col1 & 1 = 0",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 bitwise_and((int32) col1, "
                                            "(const int32) 1), (const int32) 0)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2"}, {"4"}})));
  ProcessWithFilter("col1", "col1 | 1 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 bitwise_or((int32) col1, "
                                            "(const int32) 1), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2"}, {"3"}})));
  ProcessWithFilter("col1", "col1 # 1 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 xor((int32) col1, "
                                            "(const int32) 1), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2"}})));
  ProcessWithFilter("col1", "~col1 & 2 = 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 bitwise_and(int32 bitwise_not((int32) col1), "
                                            "(const int32) 2), (const int32) 2)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1"}, {"4"}})));
}

TEST_F(FilterTestBitwise, Int8) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter("col1", "col1 & 1 = 0",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 bitwise_and((int64) col1, (const int64) 1), "
                                            "int64 castBIGINT((const int32) 0))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2"}, {"4"}})));
  ProcessWithFilter("col1", "col1 | 1 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 bitwise_or((int64) col1, (const "
                                            "int64) 1), int64 castBIGINT((const int32) 3))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2"}, {"3"}})));
  ProcessWithFilter("col1", "col1 # 1 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 xor((int64) col1, (const "
                                            "int64) 1), int64 castBIGINT((const int32) 3))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2"}})));
  ProcessWithFilter("col1", "~col1 & 2 = 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 bitwise_and(int64 bitwise_not((int64) col1), "
                                            "(const int64) 2), int64 castBIGINT((const int32) 2))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1"}, {"4"}})));
}

}  // namespace
}  // namespace tea
