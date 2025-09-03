#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

TEST_F(FilterTestBase, Miscellaneous) {
  std::string str_qwe = "qwe";
  std::string str_b2 = "b2";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_qwe, &str_b2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  std::string condition =
      "((col1 > 'asd' and col1 < 'qwe') or (col1 like 'zxc%')) and (col1 in "
      "('a1', 'b2'))";
  /* clang-format off */
  std::string expected_filter =
"{\"type\":\"and\","
  "\"left\":{\"type\":\"or\","
    "\"left\":{\"type\":\"and\","
      "\"left\":{\"type\":\"gt\",\"term\":\"col1\",\"value\":\"asd\"},"
      "\"right\":{\"type\":\"lt\",\"term\":\"col1\",\"value\":\"qwe\"}},"
    "\"right\":{\"type\":\"starts-with\",\"term\":\"col1\",\"value\":\"zxc\"}},"
  "\"right\":{\"type\":\"in\",\"term\":\"col1\",\"values\":[\"a1\",\"b2\"]}}";
  /* clang-format on */
  ProcessWithFilter(
      "col1", condition,
      ExpectedValues().SetIcebergFilters({expected_filter}).SetSelectResult(pq::ScanResult({"col1"}, {{"b2"}})));
}

TEST_F(FilterTestBase, NonConstComparison) {
  std::string a = "a";
  std::string b = "b";
  std::string c = "c";
  std::string d = "d";
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 24, 25, -1231});
  auto column2 =
      MakeInt64Column("col2", 2, OptionalVector<int64_t>{521, std::nullopt, 10'000'000'000, -10'000'000'000, 42});
  auto column3 = MakeStringColumn("col3", 3, std::vector<std::string*>{&a, &b, &c, &d, nullptr});
  PrepareData({column1, column2, column3},
              {GreenplumColumnInfo{.name = "col1", .type = "int4"}, GreenplumColumnInfo{.name = "col2", .type = "int8"},
               GreenplumColumnInfo{.name = "col3", .type = "text"}});
  ProcessWithFilter("col3, col1", "col2 > col1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than((int64) col2, int64 "
                                            "castBIGINT((int32) col1))"})
                        .SetSelectResult(pq::ScanResult({"col3", "col1"}, {{"c", "24"}, {"", "-1231"}})));
}

TEST_F(FilterTestBase, PartialFilterSimple) {
  std::string a = "a";
  std::string b = "b";
  std::string c = "c";
  std::string d = "d";
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 24, 25, -1231});
  auto column2 =
      MakeInt64Column("col2", 2, OptionalVector<int64_t>{521, std::nullopt, 10'000'000'000, -10'000'000'000, 42});
  auto column3 = MakeStringColumn("col3", 3, std::vector<std::string*>{&a, &b, &c, &d, nullptr});
  PrepareData({column1, column2, column3},
              {GreenplumColumnInfo{.name = "col1", .type = "int4"}, GreenplumColumnInfo{.name = "col2", .type = "int8"},
               GreenplumColumnInfo{.name = "col3", .type = "text"}});
  ProcessWithFilter("col3, col1", "col2 > col1 AND col1 IS DISTINCT FROM (col2 + col2)",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than((int64) col2, int64 "
                                            "castBIGINT((int32) col1))"})
                        .SetSelectResult(pq::ScanResult({"col3", "col1"}, {{"c", "24"}, {"", "-1231"}})));
}

TEST_F(FilterTestBase, PartialFilterComplex) {
  std::string a = "a";
  std::string b = "b";
  std::string c = "c";
  std::string d = "d";
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 24, 25, -1231});
  auto column2 =
      MakeInt64Column("col2", 2, OptionalVector<int64_t>{521, std::nullopt, 10'000'000'000, -10'000'000'000, 42});
  auto column3 = MakeStringColumn("col3", 3, std::vector<std::string*>{&a, &b, &c, &d, nullptr});
  PrepareData({column1, column2, column3},
              {GreenplumColumnInfo{.name = "col1", .type = "int4"}, GreenplumColumnInfo{.name = "col2", .type = "int8"},
               GreenplumColumnInfo{.name = "col3", .type = "text"}});
  ProcessWithFilter(
      "col3, col1",
      "col2 > col1 AND col1 IS DISTINCT FROM (col2 + col2) AND (col2 + 32) > 2 * col1 AND col2 IS "
      "DISTINCT FROM (col1 + col1) AND col1 != 13",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":13}"})
          .SetGandivaFilters(
              {"bool greater_than((int64) col2, int64 castBIGINT((int32) col1)) && bool greater_than(int64 "
               "AddOverflow((int64) col2, int64 castBIGINT((const int32) 32)), int64 castBIGINT(int32 "
               "MulOverflow((const int32) 2, (int32) col1))) && bool not_equal((int32) col1, (const int32) 13)"})
          .SetSelectResult(pq::ScanResult({"col3", "col1"}, {{"c", "24"}, {"", "-1231"}})));
}

}  // namespace
}  // namespace tea
