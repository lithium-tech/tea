#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterComparisonOperatorTest : public FilterTestBase {};

TEST_F(FilterComparisonOperatorTest, Boolean) {
  auto column = MakeBoolColumn("col1", 1, OptionalVector<bool>{std::nullopt, false, true});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "bool"}});
  std::string select_columns = "col1";
  std::vector<std::string> conditions_true = {"col1", "col1 = true", "col1 != false"};
  std::set<std::string> expected_filters_true = {"{\"type\":\"eq\",\"term\":\"col1\",\"value\":true}",
                                                 "{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":false}",
                                                 "{\"type\":\"not\",\"child\":{\"type\":\"eq\",\"term\":"
                                                 "\"col1\",\"value\":false}}",
                                                 "{\"type\":\"not\",\"child\":{\"type\":\"not-eq\",\"term\":"
                                                 "\"col1\",\"value\":true}}"};
  pq::ScanResult expected_select_result_true({"col1"}, {{"t"}});
#if PG_VERSION_MAJOR >= 9
  std::set<std::string> expected_gandiva_filters_true = {"(bool) col1"};
#else
  std::set<std::string> expected_gandiva_filters_true = {"(bool) col1", "",
                                                         "bool not_equal((bool) col1, (const bool) 0)"};
#endif
  for (const auto& true_condition : conditions_true) {
    ProcessWithFilter(select_columns, true_condition,
                      ExpectedValues()
                          .SetIcebergFilters(expected_filters_true)
                          .SetSelectResult(expected_select_result_true)
                          .SetGandivaFilters(expected_gandiva_filters_true));
  }

  std::vector<std::string> conditions_false = {"not col1", "col1 = false", "col1 != true"};
  std::set<std::string> expected_filters_false = {"{\"type\":\"eq\",\"term\":\"col1\",\"value\":false}",
                                                  "{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":true}",
                                                  "{\"type\":\"not\",\"child\":{\"type\":\"not-eq\",\"term\":"
                                                  "\"col1\",\"value\":false}}",
                                                  "{\"type\":\"not\",\"child\":{\"type\":\"eq\",\"term\":"
                                                  "\"col1\",\"value\":true}}"};
  pq::ScanResult expected_select_result_false({"col1"}, {{"f"}});
#if PG_VERSION_MAJOR >= 9
  std::set<std::string> expected_gandiva_filters_false = {"bool not((bool) col1)"};
#else
  std::set<std::string> expected_gandiva_filters_false = {"bool not((bool) col1)", "",
                                                          "bool not_equal((bool) col1, (const bool) 1)"};
#endif
  for (const auto& false_condition : conditions_false) {
    ProcessWithFilter(select_columns, false_condition,
                      ExpectedValues()
                          .SetIcebergFilters(expected_filters_false)
                          .SetSelectResult(expected_select_result_false)
                          .SetGandivaFilters(expected_gandiva_filters_false));
  }
}

TEST_F(FilterComparisonOperatorTest, Int2) {
  auto column = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int2"}});
  ProcessWithFilter("col1", "col1 < 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than((int16) col1, (const int16) 123)"}));
  ProcessWithFilter("col1", "col1 <= 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((int16) col1, (const int16) 123)"}));
  ProcessWithFilter("col1", "col1 > 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than((int16) col1, (const int16) 123)"}));
  ProcessWithFilter("col1", "col1 >= 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((int16) col1, "
                                            "(const int16) 123)"}));
  ProcessWithFilter("col1", "col1 = 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal((int16) col1, (const int16) 123)"}));
  ProcessWithFilter("col1", "col1 != 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal((int16) col1, (const int16) 123)"}));
}

TEST_F(FilterComparisonOperatorTest, Int4) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter("col1", "col1 < 123",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than((int32) col1, (const int32) 123)"}));
  ProcessWithFilter("col1", "col1 <= 123",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((int32) col1, (const int32) 123)"}));
  ProcessWithFilter("col1", "col1 > 123",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than((int32) col1, (const int32) 123)"}));
  ProcessWithFilter("col1", "col1 >= 123",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((int32) col1, "
                                            "(const int32) 123)"}));
  ProcessWithFilter("col1", "col1 = 123",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal((int32) col1, (const int32) 123)"}));
  ProcessWithFilter("col1", "col1 != 123",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal((int32) col1, (const int32) 123)"}));
}

TEST_F(FilterComparisonOperatorTest, Int8) {
  auto column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter("col1", "col1 < 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than((int64) col1, (const int64) 123)"}));
  ProcessWithFilter("col1", "col1 <= 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((int64) col1, (const int64) 123)"}));
  ProcessWithFilter("col1", "col1 > 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than((int64) col1, (const int64) 123)"}));
  ProcessWithFilter("col1", "col1 >= 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((int64) col1, "
                                            "(const int64) 123)"}));
  ProcessWithFilter("col1", "col1 = 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal((int64) col1, (const int64) 123)"}));
  ProcessWithFilter("col1", "col1 != 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal((int64) col1, (const int64) 123)"}));
}

TEST_F(FilterComparisonOperatorTest, Int42) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter("col1", "col1 < 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than((int32) col1, int32 "
                                            "castINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 <= 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((int32) col1, int32 "
                                            "castINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 > 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than((int32) col1, int32 "
                                            "castINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 >= 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((int32) col1, "
                                            "int32 castINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 = 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal((int32) col1, int32 castINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 != 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal((int32) col1, int32 "
                                            "castINT((const int16) 123))"}));
}

TEST_F(FilterComparisonOperatorTest, Int24) {
  auto column = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int2"}});
  ProcessWithFilter("col1", "col1 < 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than(int32 castINT((int16) col1), "
                                            "(const int32) 123)"}));
  ProcessWithFilter("col1", "col1 <= 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to(int32 "
                                            "castINT((int16) col1), (const int32) 123)"}));
  ProcessWithFilter("col1", "col1 > 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than(int32 castINT((int16) col1), "
                                            "(const int32) 123)"}));
  ProcessWithFilter("col1", "col1 >= 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to(int32 "
                                            "castINT((int16) col1), (const int32) 123)"}));
  ProcessWithFilter("col1", "col1 = 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal(int32 castINT((int16) col1), (const int32) 123)"}));
  ProcessWithFilter("col1", "col1 != 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal(int32 castINT((int16) col1), "
                                            "(const int32) 123)"}));
}

TEST_F(FilterComparisonOperatorTest, Int28) {
  auto column = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int2"}});
  ProcessWithFilter("col1", "col1 < 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than(int64 castBIGINT((int16) col1), "
                                            "(const int64) 123)"}));
  ProcessWithFilter("col1", "col1 <= 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to(int64 "
                                            "castBIGINT((int16) col1), (const int64) 123)"}));
  ProcessWithFilter("col1", "col1 > 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than(int64 castBIGINT((int16) "
                                            "col1), (const int64) 123)"}));
  ProcessWithFilter("col1", "col1 >= 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to(int64 "
                                            "castBIGINT((int16) col1), (const int64) 123)"}));
  ProcessWithFilter("col1", "col1 = 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal(int64 castBIGINT((int16) col1), "
                                            "(const int64) 123)"}));
  ProcessWithFilter("col1", "col1 != 123::int8",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal(int64 castBIGINT((int16) col1), "
                                            "(const int64) 123)"}));
}

TEST_F(FilterComparisonOperatorTest, Int84) {
  auto column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter("col1", "col1 < 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than((int64) col1, int64 "
                                            "castBIGINT((const int32) 123))"}));
  ProcessWithFilter("col1", "col1 <= 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((int64) col1, int64 "
                                            "castBIGINT((const int32) 123))"}));
  ProcessWithFilter("col1", "col1 > 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than((int64) col1, int64 "
                                            "castBIGINT((const int32) 123))"}));
  ProcessWithFilter("col1", "col1 >= 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((int64) col1, int64 "
                                            "castBIGINT((const int32) 123))"}));
  ProcessWithFilter("col1", "col1 = 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal((int64) col1, int64 "
                                            "castBIGINT((const int32) 123))"}));
  ProcessWithFilter("col1", "col1 != 123::int4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal((int64) col1, int64 "
                                            "castBIGINT((const int32) 123))"}));
}

TEST_F(FilterComparisonOperatorTest, Int82) {
  auto column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 122, 123, 124});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter("col1", "col1 < 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than((int64) col1, int64 "
                                            "castBIGINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 <= 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((int64) col1, int64 "
                                            "castBIGINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 > 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool greater_than((int64) col1, int64 "
                                            "castBIGINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 >= 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((int64) col1, int64 "
                                            "castBIGINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 = 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal((int64) col1, int64 "
                                            "castBIGINT((const int16) 123))"}));
  ProcessWithFilter("col1", "col1 != 123::int2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal((int64) col1, int64 "
                                            "castBIGINT((const int16) 123))"}));
}

TEST_F(FilterComparisonOperatorTest, Int48) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 122, 123, 124});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{std::nullopt, 123, 123, 123});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col1", "col2 > col1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool greater_than(int64 castBIGINT((int32) "
                                            "col2), (int64) col1)"}));
  ProcessWithFilter("col1", "col2 >= col1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"123"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to(int64 "
                                            "castBIGINT((int32) col2), (int64) col1)"}));
  ProcessWithFilter("col1", "col2 < col1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}}))
                        .SetGandivaFilters({"bool less_than(int64 castBIGINT((int32) col2), "
                                            "(int64) col1)"}));
  ProcessWithFilter("col1", "col2 <= col1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"124"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to(int64 "
                                            "castBIGINT((int32) col2), (int64) col1)"}));
  ProcessWithFilter("col1", "col2 = col1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}}))
                        .SetGandivaFilters({"bool equal(int64 castBIGINT((int32) col2), (int64) col1)",
                                            "bool equal(int64 castBIGINT((int32) col2), (int64) col1) && "
                                            "bool equal((int64) col1, int64 castBIGINT((int32) col2))"}));
  ProcessWithFilter("col1", "col2 != col1",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}, {"124"}}))
                        .SetGandivaFilters({"bool not_equal(int64 castBIGINT((int32) col2), "
                                            "(int64) col1)"}));
}

TEST_F(FilterComparisonOperatorTest, Text) {
  std::string str_abc = "abc";
  std::string str_abd = "abd";
  std::string str_abe = "abe";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_abc, &str_abd, &str_abe});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 < 'abd'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":\"abd\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abc"}}))
                        .SetGandivaFilters({"bool less_than((string) col1, (const string) 'abd')"}));
  ProcessWithFilter("col1", "col1 <= 'abd'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":\"abd\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abc"}, {"abd"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((string) col1, "
                                            "(const string) 'abd')"}));
  ProcessWithFilter("col1", "col1 > 'abd'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":\"abd\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abe"}}))
                        .SetGandivaFilters({"bool greater_than((string) col1, (const string) 'abd')"}));
  ProcessWithFilter("col1", "col1 >= 'abd'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":\"abd\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abd"}, {"abe"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((string) col1, "
                                            "(const string) 'abd')"}));
  ProcessWithFilter("col1", "col1 = 'abd'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":\"abd\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abd"}}))
                        .SetGandivaFilters({"bool equal((string) col1, (const string) 'abd')"}));
  ProcessWithFilter("col1", "col1 != 'abd'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":\"abd\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abc"}, {"abe"}}))
                        .SetGandivaFilters({"bool not_equal((string) col1, (const string) 'abd')"}));
}

#if 0
TEST_F(FilterComparisonOperatorTest, Float4) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2.99, 3, 3.01});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "col1 < 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}}))
                        .SetGandivaFilters({"bool less_than((float) col1, (const float) 3 raw(40400000))"}));
  ProcessWithFilter("col1", "col1 <= 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((float) col1, (const "
                                            "float) 3 raw(40400000))"}));
  ProcessWithFilter("col1", "col1 > 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3.01"}}))
                        .SetGandivaFilters({"bool greater_than((float) col1, (const float) 3 "
                                            "raw(40400000))"}));
  ProcessWithFilter("col1", "col1 >= 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}, {"3.01"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((float) col1, "
                                            "(const float) 3 raw(40400000))"}));
  ProcessWithFilter("col1", "col1 = 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}}))
                        .SetGandivaFilters({"bool equal((float) col1, (const float) 3 raw(40400000))"}));
  ProcessWithFilter("col1", "col1 != 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3.01"}}))
                        .SetGandivaFilters({"bool not_equal((float) col1, (const float) 3 raw(40400000))"}));
}

TEST_F(FilterComparisonOperatorTest, Float48) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2.99, 3, 3.01});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "col1 < 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}}))
                        .SetGandivaFilters({"bool less_than(double castFLOAT8((float) col1), "
                                            "(const double) 3 raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 <= 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to(double castFLOAT8((float) col1), "
                                            "(const double) 3 raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 > 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3.01"}}))
                        .SetGandivaFilters({"bool greater_than(double castFLOAT8((float) col1), "
                                            "(const double) 3 raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 >= 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}, {"3.01"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to(double castFLOAT8((float) col1), "
                                            "(const double) 3 raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}}))
                        .SetGandivaFilters({"bool equal(double castFLOAT8((float) col1), "
                                            "(const double) 3 raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 != 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3.01"}}))
                        .SetGandivaFilters({"bool not_equal(double castFLOAT8((float) col1), "
                                            "(const double) 3 raw(4008000000000000))"}));
}

TEST_F(FilterComparisonOperatorTest, Float84) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{std::nullopt, 2.99, 3, 3.01});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float8"}});
  ProcessWithFilter("col1", "col1 < 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}}))
                        .SetGandivaFilters({"bool less_than((double) col1, double "
                                            "castFLOAT8((const float) 3 raw(40400000)))"}));
  ProcessWithFilter("col1", "col1 <= 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((double) col1, double "
                                            "castFLOAT8((const float) 3 raw(40400000)))"}));
  ProcessWithFilter("col1", "col1 > 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3.01"}}))
                        .SetGandivaFilters({"bool greater_than((double) col1, double "
                                            "castFLOAT8((const float) 3 raw(40400000)))"}));
  ProcessWithFilter("col1", "col1 >= 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}, {"3.01"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((double) col1, double "
                                            "castFLOAT8((const float) 3 raw(40400000)))"}));
  ProcessWithFilter("col1", "col1 = 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}}))
                        .SetGandivaFilters({"bool equal((double) col1, double "
                                            "castFLOAT8((const float) 3 raw(40400000)))"}));
  ProcessWithFilter("col1", "col1 != 3::float4",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3.01"}}))
                        .SetGandivaFilters({"bool not_equal((double) col1, double "
                                            "castFLOAT8((const float) 3 raw(40400000)))"}));
}

TEST_F(FilterComparisonOperatorTest, Float8) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{std::nullopt, 2.99, 3, 3.01});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float8"}});
  ProcessWithFilter("col1", "col1 < 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}}))
                        .SetGandivaFilters({"bool less_than((double) col1, (const double) 3 "
                                            "raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 <= 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3"}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((double) col1, (const double) 3 "
                                            "raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 > 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3.01"}}))
                        .SetGandivaFilters({"bool greater_than((double) col1, (const double) 3 "
                                            "raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 >= 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}, {"3.01"}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((double) col1, (const double) 3 "
                                            "raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 = 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"3"}}))
                        .SetGandivaFilters({"bool equal((double) col1, (const double) 3 "
                                            "raw(4008000000000000))"}));
  ProcessWithFilter("col1", "col1 != 3",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":3.0}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.99"}, {"3.01"}}))
                        .SetGandivaFilters({"bool not_equal((double) col1, (const double) 3 "
                                            "raw(4008000000000000))"}));
}
#endif

TEST_F(FilterComparisonOperatorTest, Date) {
  std::string day0 = "1970-01-01";
  std::string day1 = "1970-01-02";
  std::string day2 = "1970-01-03";
  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 0, 1, 2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "date"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter("col1", "col1 < '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":1}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day0}}))
                        .SetGandivaFilters({"bool less_than((date32[day]) col1, date32[day] "
                                            "castDate((const int32) 1))"}));
  ProcessWithFilter("col1", "col1 <= '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day0}, {day1}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((date32[day]) col1, date32[day] "
                                            "castDate((const int32) 1))"}));
  ProcessWithFilter("col1", "col1 > '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":1}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day2}}))
                        .SetGandivaFilters({"bool greater_than((date32[day]) col1, date32[day] "
                                            "castDate((const int32) 1))"}));
  ProcessWithFilter("col1", "col1 >= '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day1}, {day2}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((date32[day]) col1, date32[day] "
                                            "castDate((const int32) 1))"}));
  ProcessWithFilter("col1", "col1 = '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day1}}))
                        .SetGandivaFilters({"bool equal((date32[day]) col1, date32[day] "
                                            "castDate((const int32) 1))"}));
  ProcessWithFilter("col1", "col1 != '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day0}, {day2}}))
                        .SetGandivaFilters({"bool not_equal((date32[day]) col1, date32[day] "
                                            "castDate((const int32) 1))"}));
}

TEST_F(FilterComparisonOperatorTest, TimestampToDate) {
  std::string day0 = "1970-01-01";
  std::string day1 = "1970-01-02";
  std::string day2 = "1970-01-03";
  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 0, 1, 2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "date"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter("col1", "col1 < '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":1}"})
                        .SetGandivaFilters({"bool less_than((date32[day]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day0}})));
  ProcessWithFilter("col1", "col1 <= '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetGandivaFilters({"bool less_than_or_equal_to((date32[day]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day0}, {day1}})));
  ProcessWithFilter("col1", "col1 > '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":1}"})
                        .SetGandivaFilters({"bool greater_than((date32[day]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day2}})));
  ProcessWithFilter("col1", "col1 >= '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetGandivaFilters({"bool greater_than_or_equal_to((date32[day]) col1, "
                                            "timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day1}, {day2}})));
  ProcessWithFilter("col1", "col1 = '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetGandivaFilters({"bool equal((date32[day]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day1}})));
  ProcessWithFilter("col1", "col1 != '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":1}"})
                        .SetGandivaFilters({"bool not_equal((date32[day]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{day0}, {day2}})));
}

TEST_F(FilterComparisonOperatorTest, TimestamptzToDate) {
  std::string day0 = "1970-01-01";
  std::string day1 = "1970-01-02";
  std::string day2 = "1970-01-03";
  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 0, 1, 2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "date"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter(
      "col1", "col1 < '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":1}"})
          .SetGandivaFilters({"bool less_than((date32[day]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{day0}})));
  ProcessWithFilter(
      "col1", "col1 <= '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":1}"})
          .SetGandivaFilters(
              {"bool less_than_or_equal_to((date32[day]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
               "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{day0}, {day1}})));
  ProcessWithFilter(
      "col1", "col1 > '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":1}"})
          .SetGandivaFilters({"bool greater_than((date32[day]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{day2}})));
  ProcessWithFilter(
      "col1", "col1 >= '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":1}"})
          .SetGandivaFilters(
              {"bool greater_than_or_equal_to((date32[day]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
               "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{day1}, {day2}})));
  ProcessWithFilter(
      "col1", "col1 = '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":1}"})
          .SetGandivaFilters({"bool equal((date32[day]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{day1}})));
  ProcessWithFilter(
      "col1", "col1 != '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":1}"})
          .SetGandivaFilters({"bool not_equal((date32[day]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{day0}, {day2}})));
}

TEST_F(FilterComparisonOperatorTest, Time) {
  std::string time122 = "00:00:00.000122";
  std::string time123 = "00:00:00.000123";
  std::string time124 = "00:00:00.000124";
  auto column1 = MakeTimeColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 122, 123, 124});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "time"}});
  ProcessWithFilter("col1", "col1 < '00:00:00.000123'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{time122}})));
  ProcessWithFilter("col1", "col1 <= '00:00:00.000123'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{time122}, {time123}})));
  ProcessWithFilter("col1", "col1 > '00:00:00.000123'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{time124}})));
  ProcessWithFilter("col1", "col1 >= '00:00:00.000123'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{time123}, {time124}})));
  ProcessWithFilter("col1", "col1 = '00:00:00.000123'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{time123}})));
  ProcessWithFilter("col1", "col1 != '00:00:00.000123'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{time122}, {time124}})));
}

TEST_F(FilterComparisonOperatorTest, DateToTimestamp) {
  std::string timestamp0 = "1970-01-01 00:00:00";
  std::string timestamp1 = "1970-01-02 00:00:00";
  std::string timestamp2 = "1970-01-03 00:00:00";
  auto column1 = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{0, 86400000000ll, 86400000000ll * 2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter("col1", "col1 < '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool less_than((timestamp[us]) col1, "
                                            "date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}})));
  ProcessWithFilter("col1", "col1 <= '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool less_than_or_equal_to((timestamp[us]) "
                                            "col1, date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}, {timestamp1}})));
  ProcessWithFilter("col1", "col1 > '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool greater_than((timestamp[us]) col1, "
                                            "date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp2}})));
  ProcessWithFilter("col1", "col1 >= '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool greater_than_or_equal_to((timestamp[us]) "
                                            "col1, date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp1}, {timestamp2}})));
  ProcessWithFilter("col1", "col1 = '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool equal((timestamp[us]) col1, date32[day] "
                                            "castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp1}})));
  ProcessWithFilter("col1", "col1 != '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool not_equal((timestamp[us]) col1, "
                                            "date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}, {timestamp2}})));
}

TEST_F(FilterComparisonOperatorTest, Timestamp) {
  std::string timestamp0 = "1970-01-01 00:00:00";
  std::string timestamp1 = "1970-01-02 00:00:00";
  std::string timestamp2 = "1970-01-03 00:00:00";
  auto column1 = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{0, 86400000000ll, 86400000000ll * 2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter("col1", "col1 < '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool less_than((timestamp[us]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}})));
  ProcessWithFilter("col1", "col1 <= '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool less_than_or_equal_to((timestamp[us]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}, {timestamp1}})));
  ProcessWithFilter("col1", "col1 > '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool greater_than((timestamp[us]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp2}})));
  ProcessWithFilter("col1", "col1 >= '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool greater_than_or_equal_to((timestamp[us]) col1, "
                                            "timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp1}, {timestamp2}})));
  ProcessWithFilter("col1", "col1 = '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool equal((timestamp[us]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp1}})));
  ProcessWithFilter("col1", "col1 != '1970-01-02'::timestamp",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":86400000000}"})
                        .SetGandivaFilters({"bool not_equal((timestamp[us]) col1, timestamp[us] "
                                            "castTIMESTAMP((const int64) 86400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}, {timestamp2}})));
}

TEST_F(FilterComparisonOperatorTest, TimestamptzToTimestamp) {
  std::string timestamp0 = "1970-01-01 00:00:00";
  std::string timestamp1 = "1970-01-02 00:00:00";
  std::string timestamp2 = "1970-01-03 00:00:00";
  auto column1 = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{0, 86400000000ll, 86400000000ll * 2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter(
      "col1", "col1 < '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":86400000000}"})
          .SetGandivaFilters({"bool less_than((timestamp[us]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}})));
  ProcessWithFilter(
      "col1", "col1 <= '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":86400000000}"})
          .SetGandivaFilters(
              {"bool less_than_or_equal_to((timestamp[us]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
               "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}, {timestamp1}})));
  ProcessWithFilter(
      "col1", "col1 >'1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":86400000000}"})
          .SetGandivaFilters(
              {"bool greater_than((timestamp[us]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
               "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp2}})));
  ProcessWithFilter(
      "col1", "col1 >= '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":86400000000}"})
          .SetGandivaFilters(
              {"bool greater_than_or_equal_to((timestamp[us]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
               "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp1}, {timestamp2}})));
  ProcessWithFilter(
      "col1", "col1 = '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":86400000000}"})
          .SetGandivaFilters({"bool equal((timestamp[us]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp1}})));
  ProcessWithFilter(
      "col1", "col1 != '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":86400000000}"})
          .SetGandivaFilters({"bool not_equal((timestamp[us]) col1, timestamp[us] castTIMESTAMP(timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000), (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamp0}, {timestamp2}})));
}

TEST_F(FilterComparisonOperatorTest, Timestamptz) {
  std::string timestamptz0 = "1970-01-01 00:00:00+07";
  std::string timestamptz1 = "1970-01-02 00:00:00+07";
  std::string timestamptz2 = "1970-01-03 00:00:00+07";
  auto column1 = MakeTimestamptzColumn(
      "col1", 1,
      OptionalVector<int64_t>{std::nullopt, 61200000000 - 86400000000ll, 61200000000, 61200000000 + 86400000000ll});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter("col1", "col1 < '1970-01-02 04:00:00+11'::timestamptz",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":61200000000}"})
                        .SetGandivaFilters({"bool less_than((timestamp[us, tz=UTC]) col1, timestamp[us, tz=UTC] "
                                            "castTIMESTAMPTZ((const int64) 61200000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}})));
  ProcessWithFilter(
      "col1", "col1 <= '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool less_than_or_equal_to((timestamp[us, tz=UTC]) col1, timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}, {timestamptz1}})));
  ProcessWithFilter("col1", "col1 > '1970-01-02 04:00:00+11'::timestamptz",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":61200000000}"})
                        .SetGandivaFilters({"bool greater_than((timestamp[us, tz=UTC]) col1, timestamp[us, tz=UTC] "
                                            "castTIMESTAMPTZ((const int64) 61200000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz2}})));
  ProcessWithFilter(
      "col1", "col1 >= '1970-01-02 04:00:00+11'::timestamptz",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool greater_than_or_equal_to((timestamp[us, tz=UTC]) col1, timestamp[us, tz=UTC] "
                              "castTIMESTAMPTZ((const int64) 61200000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz1}, {timestamptz2}})));
  ProcessWithFilter("col1", "col1 = '1970-01-02 04:00:00+11'::timestamptz",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":61200000000}"})
                        .SetGandivaFilters({"bool equal((timestamp[us, tz=UTC]) col1, timestamp[us, tz=UTC] "
                                            "castTIMESTAMPTZ((const int64) 61200000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz1}})));
  ProcessWithFilter("col1", "col1 != '1970-01-02 04:00:00+11'::timestamptz",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":61200000000}"})
                        .SetGandivaFilters({"bool not_equal((timestamp[us, tz=UTC]) col1, timestamp[us, tz=UTC] "
                                            "castTIMESTAMPTZ((const int64) 61200000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}, {timestamptz2}})));
}

TEST_F(FilterComparisonOperatorTest, DateToTimestamptz) {
  std::string timestamptz0 = "1970-01-01 00:00:00+07";
  std::string timestamptz1 = "1970-01-02 00:00:00+07";
  std::string timestamptz2 = "1970-01-03 00:00:00+07";
  auto column1 = MakeTimestamptzColumn(
      "col1", 1,
      OptionalVector<int64_t>{std::nullopt, 61200000000 - 86400000000ll, 61200000000, 61200000000 + 86400000000ll});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter("col1", "col1 < '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":61200000000}"})
                        .SetGandivaFilters({"bool less_than(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, "
                                            "(const int64) 25200000000), date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}})));
  ProcessWithFilter(
      "col1", "col1 <= '1970-01-02'::date",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool less_than_or_equal_to(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, "
                              "(const int64) 25200000000), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}, {timestamptz1}})));
  ProcessWithFilter(
      "col1", "col1 > '1970-01-02'::date",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool greater_than(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, "
                              "(const int64) 25200000000), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz2}})));
  ProcessWithFilter(
      "col1", "col1 >= '1970-01-02'::date",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool greater_than_or_equal_to(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, "
                              "(const int64) 25200000000), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz1}, {timestamptz2}})));
  ProcessWithFilter("col1", "col1 = '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":61200000000}"})
                        .SetGandivaFilters({"bool equal(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, "
                                            "(const int64) 25200000000), date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz1}})));
  ProcessWithFilter("col1", "col1 != '1970-01-02'::date",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":61200000000}"})
                        .SetGandivaFilters({"bool not_equal(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, "
                                            "(const int64) 25200000000), date32[day] castDate((const int32) 1))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}, {timestamptz2}})));
}

TEST_F(FilterComparisonOperatorTest, TimestampToTimestamptz) {
  std::string timestamptz0 = "1970-01-01 00:00:00+07";
  std::string timestamptz1 = "1970-01-02 00:00:00+07";
  std::string timestamptz2 = "1970-01-03 00:00:00+07";
  auto column1 = MakeTimestamptzColumn(
      "col1", 1,
      OptionalVector<int64_t>{std::nullopt, 61200000000 - 86400000000ll, 61200000000, 61200000000 + 86400000000ll});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}});
  ProcessWithFilter(
      "col1", "col1 < '1970-01-02 00:00:00+05'::timestamp",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool less_than(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
                              "25200000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}})));
  ProcessWithFilter(
      "col1", "col1 <= '1970-01-02 00:00:00+05'::timestamp",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters(
              {"bool less_than_or_equal_to(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
               "25200000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}, {timestamptz1}})));
  ProcessWithFilter(
      "col1", "col1 > '1970-01-02 00:00:00+05'::timestamp",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters(
              {"bool greater_than(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
               "25200000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz2}})));
  ProcessWithFilter(
      "col1", "col1 >= '1970-01-02 00:00:00+05'::timestamp",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters(
              {"bool greater_than_or_equal_to(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
               "25200000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz1}, {timestamptz2}})));
  ProcessWithFilter(
      "col1", "col1 = '1970-01-02 00:00:00+05'::timestamp",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool equal(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
                              "25200000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz1}})));
  ProcessWithFilter(
      "col1", "col1 != '1970-01-02 00:00:00+05'::timestamp",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":61200000000}"})
          .SetGandivaFilters({"bool not_equal(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
                              "25200000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{timestamptz0}, {timestamptz2}})));
}

TEST_F(FilterComparisonOperatorTest, Numeric) {
  std::string num122 = "1.22";
  std::string num123 = "1.23";
  std::string num124 = "1.24";
  auto column1 = MakeNumericColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124}, 7, 2);
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}});
  ProcessWithFilter("col1", "col1 < 1.23",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":\"1.23\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{num122}}))
                        .SetGandivaFilters({"bool less_than((decimal128(7, 2)) col1, (const "
                                            "decimal128(3, 2)) 123,3,2)"}));
  ProcessWithFilter("col1", "col1 <= 1.23",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":\"1.23\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{num122}, {num123}}))
                        .SetGandivaFilters({"bool less_than_or_equal_to((decimal128(7, 2)) col1, (const "
                                            "decimal128(3, 2)) 123,3,2)"}));
  ProcessWithFilter("col1", "col1 > 1.23",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":\"1.23\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{num124}}))
                        .SetGandivaFilters({"bool greater_than((decimal128(7, 2)) col1, (const "
                                            "decimal128(3, 2)) 123,3,2)"}));
  ProcessWithFilter("col1", "col1 >= 1.23",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt-eq\",\"term\":\"col1\",\"value\":\"1.23\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{num123}, {num124}}))
                        .SetGandivaFilters({"bool greater_than_or_equal_to((decimal128(7, 2)) col1, (const "
                                            "decimal128(3, 2)) 123,3,2)"}));
  ProcessWithFilter("col1", "col1 = 1.23",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"eq\",\"term\":\"col1\",\"value\":\"1.23\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{num123}}))
                        .SetGandivaFilters({"bool equal((decimal128(7, 2)) col1, (const "
                                            "decimal128(3, 2)) 123,3,2)"}));
  ProcessWithFilter("col1", "col1 != 1.23",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-eq\",\"term\":\"col1\",\"value\":\"1.23\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{num122}, {num124}}))
                        .SetGandivaFilters({"bool not_equal((decimal128(7, 2)) col1, (const "
                                            "decimal128(3, 2)) 123,3,2)"}));
}

TEST_F(FilterComparisonOperatorTest, NumericZero) {
  std::string num122 = "-1.22";
  std::string num123 = "1.23";
  std::string num124 = "1.24";
  auto column1 = MakeNumericColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, -122, 123, 124}, 7, 2);
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}});
  ProcessWithFilter(
      "col1", "col1 <= 0",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"lt-eq\",\"term\":\"col1\",\"value\":\"0\"}"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{num122}}))
          .SetGandivaFilters({"bool less_than_or_equal_to((decimal128(7, 2)) col1, (const decimal128(1, 0)) 0,1,0)"}));
}

}  // namespace
}  // namespace tea
