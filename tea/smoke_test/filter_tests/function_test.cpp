#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/filter_tests/filter_test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class FilterTestFunction : public FilterTestBase {};

#if 0
TEST_F(FilterTestFunction, Float4) {
  auto column = MakeFloatColumn("col1", 1, OptionalVector<float>{2.75, 3, 3.25, 8, 16});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "sign(col1 - 3) = -1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double sign(double subtract(double "
                                            "castFLOAT8((float) col1), (const double) 3 "
                                            "raw(4008000000000000))), (const double) -1 "
                                            "raw(bff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.75"}})));
  ProcessWithFilter("col1", "abs(col1 - 5) <= 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(double abs(double "
                                            "subtract(double castFLOAT8((float) col1), "
                                            "(const double) 5 raw(4014000000000000))), "
                                            "(const double) 2 raw(4000000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}, {"3.25"}})));
  ProcessWithFilter("col1", "ceil(col1) = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double ceil(double castFLOAT8((float) "
                                            "col1)), (const double) 3 raw(4008000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.75"}, {"3"}})));
  ProcessWithFilter("col1", "floor(col1) = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double floor(double castFLOAT8((float) "
                                            "col1)), (const double) 3 raw(4008000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}, {"3.25"}})));
  ProcessWithFilter("col1", "round(col1) = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double BankersRound(double castFLOAT8((float) col1)), (const "
                                            "double) 3 raw(4008000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.75"}, {"3"}, {"3.25"}})));
  ProcessWithFilter("col1", "sqrt(col1) = 4",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double sqrt(double castFLOAT8((float) "
                                            "col1)), (const double) 4 raw(4010000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"16"}})));
  ProcessWithFilter("col1", "cbrt(col1) = 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double cbrt(double castFLOAT8((float) "
                                            "col1)), (const double) 2 raw(4000000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"8"}})));
  ProcessWithFilter("col1", "exp(col1) < 21",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(double exp(double castFLOAT8((float) col1)), "
                                            "(const double) 21 raw(4035000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.75"}, {"3"}})));
  ProcessWithFilter("col1", "log(col1) >= 1.0625",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than_or_equal_to(double "
                                            "log10(double castFLOAT8((float) col1)), (const "
                                            "double) 1.0625 raw(3ff1000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"16"}})));
}

TEST_F(FilterTestFunction, Round) {
  auto column = MakeFloatColumn("col1", 1, OptionalVector<float>{2.5, 3.5, 4.5, 5.5});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "round(col1) = 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double BankersRound(double castFLOAT8((float) col1)), (const "
                                            "double) 2 raw(4000000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.5"}})));
  ProcessWithFilter("col1", "round(col1) = 4",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double BankersRound(double castFLOAT8((float) col1)), (const "
                                            "double) 4 raw(4010000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3.5"}, {"4.5"}})));
}

TEST_F(FilterTestFunction, Abs) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{-3, 0, 2});
  auto column2 = MakeDoubleColumn("col2", 2, OptionalVector<double>{-3, 0, 2});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{-3, 0, 2});
  auto column4 = MakeInt64Column("col4", 4, OptionalVector<int64_t>{-3, 0, 2});
  PrepareData(
      {column1, column2, column3, column4},
      {GreenplumColumnInfo{.name = "col1", .type = "float4"}, GreenplumColumnInfo{.name = "col2", .type = "float8"},
       GreenplumColumnInfo{.name = "col3", .type = "int4"}, GreenplumColumnInfo{.name = "col4", .type = "int8"}});
  ProcessWithFilter("col1", "abs(col1) = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double castFLOAT8(float abs((float) "
                                            "col1)), (const double) 3 raw(4008000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-3"}})));
  ProcessWithFilter("col2", "abs(col2) = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(double abs((double) col2), (const "
                                            "double) 3 raw(4008000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"-3"}})));
  ProcessWithFilter("col3", "abs(col3) = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 abs((int32) col3), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col3"}, {{"-3"}})));
  ProcessWithFilter("col4", "abs(col4) = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 abs((int64) col4), int64 "
                                            "castBIGINT((const int32) 3))"})
                        .SetSelectResult(pq::ScanResult({"col4"}, {{"-3"}})));
}

TEST_F(FilterTestFunction, Trigonometry) {
  auto column1 =
      MakeFloatColumn("col1", 1, OptionalVector<float>{-3, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "sin(col1) >= 0.875",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than_or_equal_to(double sin(double "
                                            "castFLOAT8((float) col1)), (const double) 0.875 "
                                            "raw(3fec000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1.5"}, {"2"}})));
  ProcessWithFilter("col1", "cos(col1) >= 0.875",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than_or_equal_to(double cos(double "
                                            "castFLOAT8((float) col1)), (const double) 0.875 "
                                            "raw(3fec000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-0.5"}, {"0"}, {"0.5"}})));
  ProcessWithFilter("col1", "tan(col1) >= 1.0625",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than_or_equal_to(double tan(double "
                                            "castFLOAT8((float) col1)), (const double) "
                                            "1.0625 raw(3ff1000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-2"}, {"1"}, {"1.5"}})));
  ProcessWithFilter("col1", "atan(col1) <= -1.0625",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(double atan(double "
                                            "castFLOAT8((float) col1)), (const double) "
                                            "-1.0625 raw(bff1000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-3"}, {"-2.5"}, {"-2"}})));
  ProcessWithFilter("col1", "abs(atan2(1, col1) - 1.625) < 0.125",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(double abs(double subtract(double atan2((const "
                                            "double) 1 raw(3ff0000000000000), double castFLOAT8((float) "
                                            "col1)), (const double) 1.625 raw(3ffa000000000000))), (const "
                                            "double) 0.125 raw(3fc0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"0"}})));
#if PG_VERSION_MAJOR >= 9
  ProcessWithFilter("col1", "sinh(col1) <= -1.0625",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(double sinh(double "
                                            "castFLOAT8((float) col1)), (const double) "
                                            "-1.0625 raw(bff1000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-3"}, {"-2.5"}, {"-2"}, {"-1.5"}, {"-1"}})));
#else
  ProcessWithFilter("col1", "sinh(col1) <= -1.0625",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(
                        pq::ScanResult({"col1"}, {{"-3"}, {"-2.5"}, {"-2"}, {"-1.5"}, {"-1"}})));
#endif
#if PG_VERSION_MAJOR >= 9
  ProcessWithFilter("col1", "cosh(col1) <= 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(double cosh(double "
                                            "castFLOAT8((float) col1)), (const double) 2 "
                                            "raw(4000000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-1"}, {"-0.5"}, {"0"}, {"0.5"}, {"1"}})));
#else
  ProcessWithFilter("col1", "cosh(col1) <= 2",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(
                        pq::ScanResult({"col1"}, {{"-1"}, {"-0.5"}, {"0"}, {"0.5"}, {"1"}})));
#endif
#if PG_VERSION_MAJOR >= 9
  ProcessWithFilter("col1", "tanh(col1) <= -0.875",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(double tanh(double "
                                            "castFLOAT8((float) col1)), (const double) "
                                            "-0.875 raw(bfec000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-3"}, {"-2.5"}, {"-2"}, {"-1.5"}})));
#else
  ProcessWithFilter("col1", "tanh(col1) <= -0.875",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(
                        pq::ScanResult({"col1"}, {{"-3"}, {"-2.5"}, {"-2"}, {"-1.5"}})));
#endif
  ProcessWithFilter("col1", "cot(col1) >= 1.0625",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than_or_equal_to(double cot(double "
                                            "castFLOAT8((float) col1)), (const double) "
                                            "1.0625 raw(3ff1000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-3"}, {"-2.5"}, {"0"}, {"0.5"}})));
  ProcessWithFilter("col1", "CASE WHEN -1 <= col1 AND col1 <= 1 THEN asin(col1) < 1 ELSE false END",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({
                            "if (bool greater_than_or_equal_to(double castFLOAT8((float) "
                            "col1), (const double) -1 raw(bff0000000000000)) && bool "
                            "less_than_or_equal_to(double castFLOAT8((float) col1), (const "
                            "double) 1 raw(3ff0000000000000))) { bool less_than(double "
                            "asin(double castFLOAT8((float) col1)), (const double) 1 "
                            "raw(3ff0000000000000)) } else { (const bool) 0 }",
                            "if (bool less_than_or_equal_to((const double) -1 "
                            "raw(bff0000000000000), double castFLOAT8((float) col1)) && "
                            "bool less_than_or_equal_to(double castFLOAT8((float) col1), "
                            "(const double) 1 raw(3ff0000000000000))) { bool "
                            "less_than(double asin(double castFLOAT8((float) col1)), (const "
                            "double) 1 raw(3ff0000000000000)) } else { (const bool) 0 }",
                        })
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"-1"}, {"-0.5"}, {"0"}, {"0.5"}})));
  ProcessWithFilter("col1", "CASE WHEN -1 <= col1 AND col1 <= 1 THEN acos(col1) < 0.5 ELSE false END",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({
                            "if (bool greater_than_or_equal_to(double castFLOAT8((float) "
                            "col1), (const double) -1 raw(bff0000000000000)) && bool "
                            "less_than_or_equal_to(double castFLOAT8((float) col1), (const "
                            "double) 1 raw(3ff0000000000000))) { bool less_than(double "
                            "acos(double castFLOAT8((float) col1)), (const double) 0.5 "
                            "raw(3fe0000000000000)) } else { (const bool) 0 }",
                            "if (bool less_than_or_equal_to((const double) -1 "
                            "raw(bff0000000000000), double castFLOAT8((float) col1)) && "
                            "bool less_than_or_equal_to(double castFLOAT8((float) col1), "
                            "(const double) 1 raw(3ff0000000000000))) { bool "
                            "less_than(double acos(double castFLOAT8((float) col1)), (const "
                            "double) 0.5 raw(3fe0000000000000)) } else { (const bool) 0 }",
                        })
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1"}})));
}
#endif

}  // namespace
}  // namespace tea
