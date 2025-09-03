#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestArithmetic : public FilterTestBase {};

TEST_F(FilterTestArithmetic, Int2) {
  auto column1 = MakeInt16Column("col1", 1, OptionalVector<int32_t>{9, 10, 13});
  auto column2 = MakeInt16Column("col2", 2, OptionalVector<int32_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int2"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int2"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16::int2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int16 AddOverflow((int16) col1, "
                                            "(int16) col2), (const int16) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1::int2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int16 SubOverflow((int16) col1, "
                                            "(int16) col2), (const int16) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36::int2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int16 MulOverflow((int16) col1, "
                                            "(int16) col2), (const int16) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1::int2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int16 DivSafe((int16) "
                                            "col1, (int16) col2), (const int16) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 % col2 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 castINT(int16 ModSafe((int16) "
                                            "col1, (int16) col2)), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
}

TEST_F(FilterTestArithmetic, Int24) {
  auto column1 = MakeInt16Column("col1", 1, OptionalVector<int32_t>{9, 10, 13});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int2"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 AddOverflow(int32 "
                                            "castINT((int16) col1), "
                                            "(int32) col2), (const int32) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int32 SubOverflow(int32 "
                                            "castINT((int16) col1), "
                                            "(int32) col2), (const int32) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 MulOverflow(int32 "
                                            "castINT((int16) col1), "
                                            "(int32) col2), (const int32) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int32 "
                                            "DivSafe(int32 castINT((int16) "
                                            "col1), (int32) col2), (const int32) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
#if PG_VERSION_MAJOR >= 9
  ProcessWithFilter("col1, col2", "col1 % col2 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 ModSafe(int32 castINT((int16) "
                                            "col1), (int32) col2), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
#else
  ProcessWithFilter(
      "col1, col2", "col1 % col2 = 3",
      ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
#endif
}

#if PG_VERSION_MAJOR >= 9

TEST_F(FilterTestArithmetic, Int28) {
  auto column1 = MakeInt16Column("col1", 1, OptionalVector<int32_t>{9, 10, 13});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int2"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int8"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 AddOverflow(int64 "
                                            "castBIGINT((int16) col1), "
                                            "(int64) col2), (const int64) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int64 SubOverflow(int64 "
                                            "castBIGINT((int16) col1), "
                                            "(int64) col2), (const int64) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 MulOverflow(int64 "
                                            "castBIGINT((int16) "
                                            "col1), (int64) col2), (const int64) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int64 DivSafe(int64 "
                                            "castBIGINT((int16) col1), (int64) col2), (const int64) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 % col2 = 3::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 ModSafe(int64 castBIGINT((int16) "
                                            "col1), (int64) col2), (const int64) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
}

TEST_F(FilterTestArithmetic, Int82) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{9, 10, 13});
  auto column2 = MakeInt16Column("col2", 2, OptionalVector<int32_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int2"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 AddOverflow((int64) col1, int64 "
                                            "castBIGINT((int16) col2)), (const int64) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int64 SubOverflow((int64) col1, int64 "
                                            "castBIGINT((int16) col2)), (const int64) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 MulOverflow((int64) col1, int64 "
                                            "castBIGINT((int16) col2)), (const int64) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int64 "
                                            "DivSafe((int64) col1, int64 "
                                            "castBIGINT((int16) col2)), (const int64) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 % col2 = 3::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 ModSafe((int64) col1, int64 "
                                            "castBIGINT((int16) col2)), (const int64) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
}

#endif

TEST_F(FilterTestArithmetic, Int4) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{9, 10, 13});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 AddOverflow((int32) col1, "
                                            "(int32) col2), (const int32) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int32 SubOverflow((int32) col1, "
                                            "(int32) col2), (const int32) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 MulOverflow((int32) col1, "
                                            "(int32) col2), (const int32) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int32 DivSafe((int32) "
                                            "col1, (int32) col2), (const int32) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 % col2 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 ModSafe((int32) col1, (int32) "
                                            "col2), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
}

TEST_F(FilterTestArithmetic, Int8) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{9, 10, 13});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int8"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 AddOverflow((int64) "
                                            "col1, (int64) col2), int64 "
                                            "castBIGINT((const int32) 16))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int64 SubOverflow((int64) "
                                            "col1, (int64) col2), "
                                            "int64 castBIGINT((const int32) -1))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 "
                                            "MulOverflow((int64) col1, (int64) col2), "
                                            "int64 castBIGINT((const int32) 36))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int64 "
                                            "DivSafe((int64) col1, (int64) "
                                            "col2), int64 castBIGINT((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 % col2 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 ModSafe((int64) col1, (int64) col2), "
                                            "int64 castBIGINT((const int32) 3))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
}

TEST_F(FilterTestArithmetic, Int84) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{9, 10, 13});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 AddOverflow((int64) col1, int64 "
                                            "castBIGINT((int32) col2)), (const int64) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int64 SubOverflow((int64) col1, int64 "
                                            "castBIGINT((int32) col2)), (const int64) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 MulOverflow((int64) col1, int64 "
                                            "castBIGINT((int32) col2)), (const int64) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int64 "
                                            "DivSafe((int64) col1, int64 "
                                            "castBIGINT((int32) col2)), (const int64) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 % col2 = 3::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 ModSafe((int64) col1, int64 "
                                            "castBIGINT((int32) col2)), (const int64) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
}

TEST_F(FilterTestArithmetic, Int42) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{9, 10, 13});
  auto column2 = MakeInt16Column("col2", 2, OptionalVector<int32_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int2"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16::int4",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 AddOverflow((int32) col1, int32 "
                                            "castINT((int16) col2)), (const int32) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1::int4",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int32 SubOverflow((int32) col1, "
                                            "int32 castINT((int16) col2)), (const int32) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36::int4",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int32 MulOverflow((int32) col1, int32 "
                                            "castINT((int16) col2)), (const int32) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1::int4",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int32 "
                                            "DivSafe((int32) col1, int32 "
                                            "castINT((int16) col2)), (const int32) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));

#if PG_VERSION_MAJOR >= 9
  ProcessWithFilter("col1, col2", "col1 % col2 = 3::int4",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int32 ModSafe((int32) col1, int32 "
                                            "castINT((int16) col2)), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
#else
  ProcessWithFilter(
      "col1, col2", "col1 % col2 = 3::int4",
      ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
#endif
}

TEST_F(FilterTestArithmetic, Int48) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{9, 10, 13});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{4, 7, 15});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int8"}});
  ProcessWithFilter("col1, col2", "col1 + col2 > 16::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 AddOverflow(int64 "
                                            "castBIGINT((int32) col1), "
                                            "(int64) col2), (const int64) 16)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 - col2 < -1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than(int64 SubOverflow(int64 "
                                            "castBIGINT((int32) col1), "
                                            "(int64) col2), (const int64) -1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 * col2 > 36::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(int64 MulOverflow(int64 "
                                            "castBIGINT((int32) "
                                            "col1), (int64) col2), (const int64) 36)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 / col2 <= 1::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool less_than_or_equal_to(int64 DivSafe(int64 "
                                            "castBIGINT((int32) col1), (int64) col2), (const int64) 1)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}, {"13", "15"}})));
  ProcessWithFilter("col1, col2", "col1 % col2 = 3::int8",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool equal(int64 ModSafe(int64 castBIGINT((int32) "
                                            "col1), (int64) col2), (const int64) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"10", "7"}})));
}

#if 0
TEST_F(FilterTestArithmetic, Float4) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{2.75, 3, 3.25});
  auto column2 = MakeFloatColumn("col2", 2, OptionalVector<float>{3, 3, 3.75});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "float4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "float4"}});
  ProcessWithFilter("col1, col2", "col2 + col1 > 5.75",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double castFLOAT8(float add((float) col2, "
                                            "(float) col1)), (const double) 5.75 raw(4017000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 - col1 > 0",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double castFLOAT8(float subtract((float) "
                                            "col2, (float) col1)), (const double) 0 raw(0))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 * col1 > 8.5",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double castFLOAT8(float "
                                            "multiply((float) col2, (float) col1)), (const "
                                            "double) 8.5 raw(4021000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 / col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double castFLOAT8(float divide((float) col2, "
                                            "(float) col1)), (const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
}

TEST_F(FilterTestArithmetic, Float84) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{2.75, 3, 3.25});
  auto column2 = MakeFloatColumn("col2", 2, OptionalVector<float>{3, 3, 3.75});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "float8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "float4"}});
  ProcessWithFilter("col1, col2", "col2 + col1 > 5.75",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double add(double castFLOAT8((float) col2), "
                                            "(double) col1), (const double) 5.75 raw(4017000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 - col1 > 0",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double subtract(double castFLOAT8((float) "
                                            "col2), (double) col1), (const double) 0 raw(0))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 * col1 > 8.5",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double multiply(double "
                                            "castFLOAT8((float) col2), (double) col1), "
                                            "(const double) 8.5 raw(4021000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 / col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double divide(double "
                                            "castFLOAT8((float) col2), (double) col1), "
                                            "(const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
}

TEST_F(FilterTestArithmetic, Float48) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{2.75, 3, 3.25});
  auto column2 = MakeDoubleColumn("col2", 2, OptionalVector<double>{3, 3, 3.75});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "float4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "float8"}});
  ProcessWithFilter("col1, col2", "col2 + col1 > 5.75",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double add((double) col2, "
                                            "double castFLOAT8((float) col1)), (const "
                                            "double) 5.75 raw(4017000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 - col1 > 0",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double subtract((double) col2, double "
                                            "castFLOAT8((float) col1)), (const double) 0 raw(0))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 * col1 > 8.5",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double multiply((double) "
                                            "col2, double castFLOAT8((float) col1)), (const "
                                            "double) 8.5 raw(4021000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 / col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double divide((double) col2, "
                                            "double castFLOAT8((float) col1)), (const "
                                            "double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
}

TEST_F(FilterTestArithmetic, Float8) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{2.75, 3, 3.25});
  auto column2 = MakeDoubleColumn("col2", 2, OptionalVector<double>{3, 3, 3.75});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "float8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "float8"}});
  ProcessWithFilter("col1, col2", "col2 + col1 > 5.75",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double add((double) col2, (double) col1), "
                                            "(const double) 5.75 raw(4017000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 - col1 > 0",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double subtract((double) "
                                            "col2, (double) col1), (const double) 0 raw(0))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 * col1 > 8.5",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double multiply((double) col2, (double) "
                                            "col1), (const double) 8.5 raw(4021000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"3", "3"}, {"3.25", "3.75"}})));
  ProcessWithFilter("col1, col2", "col2 / col1 > 1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(double divide((double) col2, (double) col1), "
                                            "(const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"2.75", "3"}, {"3.25", "3.75"}})));
}
#endif

}  // namespace
}  // namespace tea
