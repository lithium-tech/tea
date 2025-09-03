#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestInOperator : public FilterTestBase {};

TEST_F(FilterTestInOperator, Int2) {
  auto column1 = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int2"}});
  ProcessWithFilter("col1", "col1 in (null, 126)",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"126"}})));
  ProcessWithFilter("col1", "col1 in (123, 126)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
                        .SetGandivaFilters({"if (bool isnull((int16) col1)) { (const bool) null } else { int32 "
                                            "castINT((int16) col1) IN (123, 126) }",
                                            "if (bool isnull((int16) col1)) { (const bool) null } else { int32 "
                                            "castINT((int16) col1) IN (126, 123) }"}));
  ProcessWithFilter("col1", "col1 not in (123, 126)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((int16) col1)) { (const bool) null } else { "
                                            "int32 castINT((int16) col1) IN (123, 126) })",
                                            "bool not(if (bool isnull((int16) col1)) { (const bool) null } else { "
                                            "int32 castINT((int16) col1) IN (126, 123) })"}));
}

TEST_F(FilterTestInOperator, Int4) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter("col1", "col1 in (null, 126)",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"126"}})));
  ProcessWithFilter("col1", "col1 in (123, 126)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
                        .SetGandivaFilters({"if (bool isnull((int32) col1)) { (const bool) "
                                            "null } else { (int32) col1 IN (123, 126) }",
                                            "if (bool isnull((int32) col1)) { (const bool) "
                                            "null } else { (int32) col1 IN (126, 123) }"}));
  ProcessWithFilter("col1", "col1 not in (123, 126)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((int32) col1)) { (const bool) null } "
                                            "else { (int32) col1 IN (123, 126) })",
                                            "bool not(if (bool isnull((int32) col1)) { (const bool) null } "
                                            "else { (int32) col1 IN (126, 123) })"}));
}

TEST_F(FilterTestInOperator, Int8) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter("col1", "col1 in (null, 126)",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"126"}})));
  ProcessWithFilter("col1", "col1 in (123, 126)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
                        .SetGandivaFilters({"if (bool isnull((int64) col1)) { (const bool) "
                                            "null } else { (int64) col1 IN (123, 126) }",
                                            "if (bool isnull((int64) col1)) { (const bool) "
                                            "null } else { (int64) col1 IN (126, 123) }"}));
  ProcessWithFilter("col1", "col1 not in (123, 126)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                                            "\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((int64) col1)) { (const bool) null } "
                                            "else { (int64) col1 IN (123, 126) })",
                                            "bool not(if (bool isnull((int64) col1)) { (const bool) null } "
                                            "else { (int64) col1 IN (126, 123) })"}));
}

TEST_F(FilterTestInOperator, Text) {
  std::string str_a = "a";
  std::string str_b = "b";
  std::string str_c = "c";
  std::string str_d = "d";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_a, &str_b, &str_c, &str_d});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 in (null, 'd')",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"d"}})));
  ProcessWithFilter("col1", "col1 in ('a', 'd')",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":"
                                            "[\"a\",\"d\"]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"a"}, {"d"}}))
                        .SetGandivaFilters({"if (bool isnull((string) col1)) { (const bool) "
                                            "null } else { (string) col1 IN (a, d) }",
                                            "if (bool isnull((string) col1)) { (const bool) "
                                            "null } else { (string) col1 IN (d, a) }"}));
  ProcessWithFilter("col1", "col1 not in ('a', 'd')",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                                            "\"values\":[\"a\",\"d\"]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"b"}, {"c"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((string) col1)) { (const bool) null } "
                                            "else { (string) col1 IN (a, d) })",
                                            "bool not(if (bool isnull((string) col1)) { (const bool) null } "
                                            "else { (string) col1 IN (d, a) })"}));
}

#if 0
TEST_F(FilterTestInOperator, Float4) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2.125, 2.25, 2.5, 3});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "col1 in (null, 3)",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"3"}})));
  ProcessWithFilter("col1", "col1 in (2.125, 3)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[2.125,3.0]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.125"}, {"3"}}))
                        .SetGandivaFilters({"if (bool isnull((float) col1)) { (const bool) "
                                            "null } else { (float) col1 IN (2.125, 3) }",
                                            "if (bool isnull((float) col1)) { (const bool) "
                                            "null } else { (float) col1 IN (3, 2.125) }"}));
  ProcessWithFilter("col1", "col1 not in (2.125, 3)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":"
                                            "\"col1\",\"values\":[2.125,3.0]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.25"}, {"2.5"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((float) col1)) { (const bool) null } "
                                            "else { (float) col1 IN (2.125, 3) })",
                                            "bool not(if (bool isnull((float) col1)) { (const bool) null } "
                                            "else { (float) col1 IN (3, 2.125) })"}));
}

TEST_F(FilterTestInOperator, Float8) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{std::nullopt, 2.125, 2.25, 2.5, 3});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "float8"}});
  ProcessWithFilter("col1", "col1 in (null, 3)",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"3"}})));
  ProcessWithFilter("col1", "col1 in (2.125, 3)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[2.125,3.0]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.125"}, {"3"}}))
                        .SetGandivaFilters({"if (bool isnull((double) col1)) { (const bool) "
                                            "null } else { (double) col1 IN (2.125, 3) }",
                                            "if (bool isnull((double) col1)) { (const bool) "
                                            "null } else { (double) col1 IN (3, 2.125) }"}));
  ProcessWithFilter("col1", "col1 not in (2.125, 3)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":"
                                            "\"col1\",\"values\":[2.125,3.0]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.25"}, {"2.5"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((double) col1)) { (const bool) null } "
                                            "else { (double) col1 IN (2.125, 3) })",
                                            "bool not(if (bool isnull((double) col1)) { (const bool) null } "
                                            "else { (double) col1 IN (3, 2.125) })"}));
}
#endif

TEST_F(FilterTestInOperator, Numeric) {
  auto column1 = MakeNumericColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124, 125}, 7, 2);
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}});
  ProcessWithFilter("col1", "col1 in (null, 1.25)",
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"1.25"}})));
  ProcessWithFilter("col1", "col1 in (1.22, 1.25)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":["
                                            "\"1.22\",\"1.25\"]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1.22"}, {"1.25"}})));
  ProcessWithFilter("col1", "col1 not in (1.22, 1.25)",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\",\"values\":["
                                            "\"1.22\",\"1.25\"]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1.23"}, {"1.24"}})));
}

TEST_F(FilterTestInOperator, Time) {
  auto column1 = MakeTimeColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 122, 123, 124, 125});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "time"}});
  ProcessWithFilter(
      "col1", "col1 in (null, '00:00:00.000125')",
      ExpectedValues().SetIcebergFilters({""}).SetSelectResult(pq::ScanResult({"col1"}, {{"00:00:00.000125"}})));
  ProcessWithFilter("col1", "col1 in ('00:00:00.000122', '00:00:00.000125')",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[122,125]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"00:00:00.000122"}, {"00:00:00.000125"}})));
  ProcessWithFilter("col1", "col1 not in ('00:00:00.000122', '00:00:00.000125')",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                                            "\"values\":[122,125]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"00:00:00.000123"}, {"00:00:00.000124"}})));
}

TEST_F(FilterTestInOperator, Date) {
  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 1, 2, 3, 4, 5});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "date"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  std::vector<std::string> conditions124 = {"col1 in ('1970-01-02', '1970-01-03', '1970-01-05')",
                                            "col1 in ('1970-01-02'::timestamp, '1970-01-03'::timestamp, "
                                            "'1970-01-05'::timestamp)",
                                            "col1 in ('1970-01-02 04:00:00+11'::timestamptz, "
                                            "'1970-01-03'::timestamptz,"
                                            "'1970-01-05'::timestamptz)",
                                            "col1 in ('1970-01-02', '1970-01-03'::timestamp, '1970-01-05 "
                                            "03:00:00+10'::timestamptz)"};
  std::string filter124 = {"{\"type\":\"in\",\"term\":\"col1\",\"values\":[1,2,4]}"};
  pq::ScanResult result124({"col1"}, {{"1970-01-02"}, {"1970-01-03"}, {"1970-01-05"}});
  for (const auto& condition : conditions124) {
    ProcessWithFilter("col1", condition, ExpectedValues().SetIcebergFilters({filter124}).SetSelectResult(result124));
  }

  const std::string condition_with_null = "col1 in (null, '1970-01-03', '1970-01-05')";
  pq::ScanResult result_with_null({"col1"}, {{"1970-01-03"}, {"1970-01-05"}});
  ProcessWithFilter("col1", condition_with_null,
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(result_with_null));

  std::vector<std::string> conditions35 = {"col1 not in ('1970-01-02', '1970-01-03', '1970-01-05')",
                                           "col1 not in ('1970-01-02'::timestamp, '1970-01-03'::timestamp, "
                                           "'1970-01-05'::timestamp)",
                                           "col1 not in ('1970-01-02 04:00:00+11'::timestamptz, "
                                           "'1970-01-03'::timestamptz,"
                                           "'1970-01-05'::timestamptz)",
                                           "col1 not in ('1970-01-02', '1970-01-03'::timestamp, '1970-01-05 "
                                           "03:00:00+10'::timestamptz)"};
  std::string filter35 = {"{\"type\":\"not-in\",\"term\":\"col1\",\"values\":[1,2,4]}"};
  pq::ScanResult result35({"col1"}, {{"1970-01-04"}, {"1970-01-06"}});
  for (const auto& condition : conditions35) {
    ProcessWithFilter("col1", condition, ExpectedValues().SetIcebergFilters({filter35}).SetSelectResult(result35));
  }
}

TEST_F(FilterTestInOperator, Timestamp) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{std::nullopt, 86400000000, 86400000000 * 2ll,
                                                             86400000000 * 3ll, 86400000000 * 4ll, 86400000000 * 5ll});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  std::vector<std::string> conditions124 = {"col1 in ('1970-01-02', '1970-01-03', '1970-01-05')",
                                            "col1 in ('1970-01-02'::timestamp, '1970-01-03'::timestamp, "
                                            "'1970-01-05'::timestamp)",
                                            "col1 in ('1970-01-02 04:00:00+11'::timestamptz, "
                                            "'1970-01-03'::timestamptz,"
                                            "'1970-01-05'::timestamptz)",
                                            "col1 in ('1970-01-02', '1970-01-03'::timestamp, '1970-01-05 "
                                            "03:00:00+10'::timestamptz)"};
  std::string filter124 = {
      "{\"type\":\"in\",\"term\":\"col1\",\"values\":[86400000000,"
      "172800000000,"
      "345600000000]}"};
  pq::ScanResult result124({"col1"}, {{"1970-01-02 00:00:00"}, {"1970-01-03 00:00:00"}, {"1970-01-05 00:00:00"}});
  for (const auto& condition : conditions124) {
    ProcessWithFilter("col1", condition, ExpectedValues().SetIcebergFilters({filter124}).SetSelectResult(result124));
  }

  const std::string condition_with_null = "col1 in (null, '1970-01-03', '1970-01-05')";
  pq::ScanResult result_with_null({"col1"}, {{"1970-01-03 00:00:00"}, {"1970-01-05 00:00:00"}});
  ProcessWithFilter("col1", condition_with_null,
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(result_with_null));

  std::vector<std::string> conditions35 = {"col1 not in ('1970-01-02', '1970-01-03', '1970-01-05')",
                                           "col1 not in ('1970-01-02'::timestamp, '1970-01-03'::timestamp, "
                                           "'1970-01-05'::timestamp)",
                                           "col1 not in ('1970-01-02 04:00:00+11'::timestamptz, "
                                           "'1970-01-03'::timestamptz,"
                                           "'1970-01-05'::timestamptz)",
                                           "col1 not in ('1970-01-02', '1970-01-03'::timestamp, '1970-01-05 "
                                           "03:00:00+10'::timestamptz)"};
  std::string filter35 = {
      "{\"type\":\"not-in\",\"term\":\"col1\",\"values\":[86400000000,"
      "172800000000,"
      "345600000000]}"};
  pq::ScanResult result35({"col1"}, {{"1970-01-04 00:00:00"}, {"1970-01-06 00:00:00"}});
  for (const auto& condition : conditions35) {
    ProcessWithFilter("col1", condition, ExpectedValues().SetIcebergFilters({filter35}).SetSelectResult(result35));
  }
}

TEST_F(FilterTestInOperator, TimestampWithTimeZone) {
  auto column1 = MakeTimestamptzColumn(
      "col1", 1,
      OptionalVector<int64_t>{std::nullopt, 61200000000, 61200000000 + 86400000000, 61200000000 + 86400000000 * 2ll,
                              61200000000 + 86400000000 * 3ll, 61200000000 + 86400000000 * 4ll});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  std::vector<std::string> conditions124 = {"col1 in ('1970-01-02', '1970-01-03', '1970-01-05')",
                                            "col1 in ('1970-01-02'::timestamp, '1970-01-03'::timestamp, "
                                            "'1970-01-05'::timestamp)",
                                            "col1 in ('1970-01-02 04:00:00+11'::timestamptz, "
                                            "'1970-01-03'::timestamptz,"
                                            "'1970-01-05'::timestamptz)",
                                            "col1 in ('1970-01-02', '1970-01-03'::timestamp, '1970-01-05 "
                                            "03:00:00+10'::timestamptz)"};
  std::string filter124 = {
      "{\"type\":\"in\",\"term\":\"col1\",\"values\":[61200000000,"
      "147600000000,"
      "320400000000]}"};
  pq::ScanResult result124({"col1"},
                           {{"1970-01-02 00:00:00+07"}, {"1970-01-03 00:00:00+07"}, {"1970-01-05 00:00:00+07"}});
  for (const auto& condition : conditions124) {
    ProcessWithFilter("col1", condition, ExpectedValues().SetIcebergFilters({filter124}).SetSelectResult(result124));
  }

  const std::string condition_with_null = "col1 in (null, '1970-01-03', '1970-01-05')";
  pq::ScanResult result_with_null({"col1"}, {{"1970-01-03 00:00:00+07"}, {"1970-01-05 00:00:00+07"}});
  ProcessWithFilter("col1", condition_with_null,
                    ExpectedValues().SetIcebergFilters({""}).SetSelectResult(result_with_null));

  std::vector<std::string> conditions35 = {"col1 not in ('1970-01-02', '1970-01-03', '1970-01-05')",
                                           "col1 not in ('1970-01-02'::timestamp, '1970-01-03'::timestamp, "
                                           "'1970-01-05'::timestamp)",
                                           "col1 not in ('1970-01-02 04:00:00+11'::timestamptz, "
                                           "'1970-01-03'::timestamptz,"
                                           "'1970-01-05'::timestamptz)",
                                           "col1 not in ('1970-01-02', '1970-01-03'::timestamp, '1970-01-05 "
                                           "03:00:00+10'::timestamptz)"};
  std::string filter35 = {
      "{\"type\":\"not-in\",\"term\":\"col1\",\"values\":[61200000000,"
      "147600000000,"
      "320400000000]}"};
  pq::ScanResult result35({"col1"}, {{"1970-01-04 00:00:00+07"}, {"1970-01-06 00:00:00+07"}});
  for (const auto& condition : conditions35) {
    ProcessWithFilter("col1", condition, ExpectedValues().SetIcebergFilters({filter35}).SetSelectResult(result35));
  }
}

}  // namespace
}  // namespace tea
