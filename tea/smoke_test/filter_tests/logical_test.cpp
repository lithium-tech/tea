#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

TEST_F(FilterTestBase, Or) {
  std::string str_aa = "aa";
  std::string str_bb = "bb";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_aa, &str_aa, &str_bb, &str_bb});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{std::nullopt, 10, 42, 10, 42});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "text"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  /* clang-format off */
  std::string expected_filter =
"{\"type\":\"or\","
  "\"left\":{\"type\":\"gt\",\"term\":\"col1\",\"value\":\"abc\"},"
  "\"right\":{\"type\":\"gt\",\"term\":\"col2\",\"value\":15}}";
  /* clang-format on */
  ProcessWithFilter("col1, col2", "col1 > 'abc' or col2 > 15",
                    ExpectedValues()
                        .SetIcebergFilters({expected_filter})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"aa", "42"}, {"bb", "10"}, {"bb", "42"}}))
                        .SetGandivaFilters({"bool greater_than((string) col1, (const string) 'abc') || bool "
                                            "greater_than((int32) col2, (const int32) 15)"}));
}

TEST_F(FilterTestBase, And) {
  std::string str_aa = "aa";
  std::string str_bb = "bb";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_aa, &str_aa, &str_bb, &str_bb});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{std::nullopt, 10, 42, 10, 42});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "text"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  /* clang-format off */
  std::string expected_filter =
"{\"type\":\"and\","
  "\"left\":{\"type\":\"gt\",\"term\":\"col1\",\"value\":\"abc\"},"
  "\"right\":{\"type\":\"gt\",\"term\":\"col2\",\"value\":15}}";
  /* clang-format on */
  ProcessWithFilter("col1, col2", "col1 > 'abc' and col2 > 15",
                    ExpectedValues()
                        .SetIcebergFilters({expected_filter})
                        .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"bb", "42"}}))
                        .SetGandivaFilters({"bool greater_than((string) col1, (const string) 'abc') && bool "
                                            "greater_than((int32) col2, (const int32) 15)"}));
}

TEST_F(FilterTestBase, Not) {
  std::string str_aa = "aa";
  std::string str_bb = "bb";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_aa, &str_aa, &str_bb, &str_bb});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{std::nullopt, 10, 42, 10, 42});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "text"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  /* clang-format off */
  std::string expected_filter1 =
"{\"type\":\"not\",\"child\":"
  "{\"type\":\"and\","
    "\"left\":{\"type\":\"gt\",\"term\":\"col1\",\"value\":\"abc\"},"
    "\"right\":{\"type\":\"gt\",\"term\":\"col2\",\"value\":15}}}";
  std::string expected_filter2 =
"{\"type\":\"or\","
  "\"left\":{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":\"abc\"},"
  "\"right\":{\"type\":\"lt-eq\",\"term\":\"col2\",\"value\":15}}";
  std::string expected_filter3 =
"{\"type\":\"or\","
  "\"left\":{\"type\":\"not\",\"child\":"
    "{\"type\":\"gt\",\"term\":\"col1\",\"value\":\"abc\"}},"
  "\"right\":{\"type\":\"not\",\"child\":"
    "{\"type\":\"gt\",\"term\":\"col2\",\"value\":15}}}";
  /* clang-format on */
  std::string expected_gandiva_filter1 =
      "bool not(bool greater_than((string) col1, (const string) 'abc') && "
      "bool greater_than((int32) col2, (const int32) 15))";
  std::string expected_gandiva_filter2 =
      "bool less_than_or_equal_to((string) col1, (const string) 'abc') || "
      "bool less_than_or_equal_to((int32) col2, (const int32) 15)";
  std::string expected_gandiva_filter3 =
      "bool not(bool greater_than((string) col1, (const string) 'abc')) || "
      "bool not(bool greater_than((int32) col2, (const int32) 15))";
  ProcessWithFilter(
      "col1, col2", "not (col1 > 'abc' and col2 > 15)",
      ExpectedValues()
          .SetIcebergFilters({expected_filter1, expected_filter2, expected_filter3})
          .SetSelectResult(pq::ScanResult({"col1", "col2"}, {{"aa", "10"}, {"aa", "42"}, {"bb", "10"}}))
          .SetGandivaFilters({expected_gandiva_filter1, expected_gandiva_filter2, expected_gandiva_filter3}));
}

}  // namespace
}  // namespace tea
