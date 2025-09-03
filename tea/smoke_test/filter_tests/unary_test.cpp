#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestUnary : public FilterTestBase {};

TEST_F(FilterTestUnary, Int4) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{-5, -2, 0, 2, 5});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter("col1", "@col1 > 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 abs((int32) col1), (const int32) 2)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"5"}})));
  ProcessWithFilter("col1", "-col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 negative((int32) col1), "
                                            "(const int32) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"-2"}})));
}

TEST_F(FilterTestUnary, Int8) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{-5, -2, 0, 2, 5});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter("col1", "@col1 > 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 abs((int64) col1), "
                                            "int64 castBIGINT((const int32) 2))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"5"}})));
  ProcessWithFilter("col1", "-col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 negative((int64) col1), "
                                            "int64 castBIGINT((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"-2"}})));
}

#if 0
TEST_F(FilterTestUnary, Float4) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{-5, -2.5, 0, 2.5, 5});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "@col1 > 2.5",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double castFLOAT8(float abs((float) col1)), "
                                            "(const double) 2.5 raw(4004000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"5"}})));
  ProcessWithFilter("col1", "-col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double castFLOAT8(float negative((float) "
                                            "col1)), (const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"-2.5"}})));
}

TEST_F(FilterTestUnary, Float8) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{-5, -2.5, 0, 2.5, 5});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float8"}});
  ProcessWithFilter("col1", "@col1 > 2.5",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double abs((double) col1), "
                                            "(const double) 2.5 raw(4004000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"5"}})));
  ProcessWithFilter("col1", "-col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double negative((double) "
                                            "col1), (const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-5"}, {"-2.5"}})));
}
#endif

TEST_F(FilterTestUnary, Numeric) {
  auto column1 = MakeNumericColumn("col1", 1, OptionalVector<int32_t>{-250, -125, 0, 125, 250}, 7, 2);
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}});
  ProcessWithFilter("col1", "@col1 > 1.5",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(decimal128(7, 2) abs((decimal128(7, 2)) "
                                            "col1), (const decimal128(2, 1)) 15,2,1)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-2.50"}, {"2.50"}})));
  ProcessWithFilter("col1", "-col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(decimal128(7, 2) negative((decimal128(7, 2)) "
                                            "col1), (const decimal128(1, 0)) 1,1,0)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-2.50"}, {"-1.25"}})));
}

}  // namespace
}  // namespace tea
