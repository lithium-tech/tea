#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/filter_tests/filter_test_base.h"
#include "tea/smoke_test/pq.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class FilterTestAnyAllOperators : public FilterTestBase {};

TEST_F(FilterTestAnyAllOperators, Text) {
  std::string str_a = "a";
  std::string str_b = "b";
  std::string str_c = "c";
  std::string str_d = "d";

  auto column = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_a, &str_b, &str_c, &str_d});
  PrepareData(std::vector<ParquetColumn>{column},
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 = any (array['a', 'd'])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[\"a\",\"d\"]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"a"}, {"d"}}))
                        .SetGandivaFilters({"if (bool isnull((string) col1)) { (const bool) "
                                            "null } else { (string) col1 IN (a, d) }",
                                            "if (bool isnull((string) col1)) { (const bool) "
                                            "null } else { (string) col1 IN (d, a) }"}));
  ProcessWithFilter("col1", "col1 != all (array['a', 'd'])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                                            "\"values\":[\"a\",\"d\"]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"b"}, {"c"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((string) col1)) { (const bool) null } "
                                            "else { (string) col1 IN (a, d) })",
                                            "bool not(if (bool isnull((string) col1)) { (const bool) null } "
                                            "else { (string) col1 IN (d, a) })"}));
}

TEST_F(FilterTestAnyAllOperators, Int84) {
  auto column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter("col1", "col1 = any (array[123, 126]::int4[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
                        .SetGandivaFilters({"if (bool isnull((int64) col1)) { (const bool) "
                                            "null } else { (int64) col1 IN (123, 126) }",
                                            "if (bool isnull((int64) col1)) { (const bool) "
                                            "null } else { (int64) col1 IN (126, 123) }"}));
  ProcessWithFilter("col1", "col1 != all (array[123, 126]::int4[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                                            "\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((int64) col1)) { (const bool) null } "
                                            "else { (int64) col1 IN (123, 126) })",
                                            "bool not(if (bool isnull((int64) col1)) { (const bool) null } "
                                            "else { (int64) col1 IN (126, 123) })"}));
}

TEST_F(FilterTestAnyAllOperators, Int48) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter("col1", "col1 = any (array[123, 126]::int8[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
                        .SetGandivaFilters({"if (bool isnull((int32) col1)) { (const bool) null } else { int64 "
                                            "castBIGINT((int32) col1) IN (123, 126) }",
                                            "if (bool isnull((int32) col1)) { (const bool) null } else { int64 "
                                            "castBIGINT((int32) col1) IN (126, 123) }"}));
  ProcessWithFilter("col1", "col1 != all (array[123, 126]::int8[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                                            "\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((int32) col1)) { (const bool) null } else { "
                                            "int64 castBIGINT((int32) col1) IN (123, 126) })",
                                            "bool not(if (bool isnull((int32) col1)) { (const bool) null } else { "
                                            "int64 castBIGINT((int32) col1) IN (126, 123) })"}));
}

TEST_F(FilterTestAnyAllOperators, Int24) {
  auto column = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int2"}});
  ProcessWithFilter("col1", "col1 = any (array[123, 126]::int4[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
                        .SetGandivaFilters({"if (bool isnull((int16) col1)) { (const bool) null } else { int32 "
                                            "castINT((int16) col1) IN (123, 126) }",
                                            "if (bool isnull((int16) col1)) { (const bool) null } else { int32 "
                                            "castINT((int16) col1) IN (126, 123) }"}));
  ProcessWithFilter(
      "col1", "col1 != all (array[123, 126]::int4[])",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                              "\"values\":[123,126]}"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
          .SetGandivaFilters({"bool not(if (bool isnull((int16) col1)) { (const bool) null } else { int32 "
                              "castINT((int16) col1) IN (123, 126) })",
                              "bool not(if (bool isnull((int16) col1)) { (const bool) null } else { int32 "
                              "castINT((int16) col1) IN (126, 123) })"}));
}

TEST_F(FilterTestAnyAllOperators, Int28) {
  auto column = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int2"}});
  ProcessWithFilter("col1", "col1 = any (array[123, 126]::int8[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
                        .SetGandivaFilters({"if (bool isnull((int16) col1)) { (const bool) null } else { int64 "
                                            "castBIGINT((int16) col1) IN (123, 126) }",
                                            "if (bool isnull((int16) col1)) { (const bool) null } else { int64 "
                                            "castBIGINT((int16) col1) IN (126, 123) }"}));
  ProcessWithFilter("col1", "col1 != all (array[123, 126]::int8[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                                            "\"values\":[123,126]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((int16) col1)) { (const bool) null } else { "
                                            "int64 castBIGINT((int16) col1) IN (123, 126) })",
                                            "bool not(if (bool isnull((int16) col1)) { (const bool) null } else { "
                                            "int64 castBIGINT((int16) col1) IN (126, 123) })"}));
}

TEST_F(FilterTestAnyAllOperators, Int42) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter(
      "col1", "col1 = any (array[123, 126]::int2[])",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
          .SetGandivaFilters(
              {"if (bool isnull((int32) col1)) { (const bool) null } else { (int32) col1 IN (123, 126) }",
               "if (bool isnull((int32) col1)) { (const bool) null } else { (int32) col1 IN (126, 123) }"}));
  ProcessWithFilter(
      "col1", "col1 != all (array[123, 126]::int2[])",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                              "\"values\":[123,126]}"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
          .SetGandivaFilters(
              {"bool not(if (bool isnull((int32) col1)) { (const bool) null } else { (int32) col1 IN (123, 126) })",
               "bool not(if (bool isnull((int32) col1)) { (const bool) null } else { (int32) col1 IN (126, 123) })"}));
}

TEST_F(FilterTestAnyAllOperators, Int82) {
  auto column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 124, 125, 126});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "int8"}});
  ProcessWithFilter(
      "col1", "col1 = any (array[123, 126]::int2[])",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[123,126]}"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"123"}, {"126"}}))
          .SetGandivaFilters(
              {"if (bool isnull((int64) col1)) { (const bool) null } else { (int64) col1 IN (123, 126) }",
               "if (bool isnull((int64) col1)) { (const bool) null } else { (int64) col1 IN (126, 123) }"}));
  ProcessWithFilter(
      "col1", "col1 != all (array[123, 126]::int2[])",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":\"col1\","
                              "\"values\":[123,126]}"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"124"}, {"125"}}))
          .SetGandivaFilters(
              {"bool not(if (bool isnull((int64) col1)) { (const bool) null } else { (int64) col1 IN (123, 126) })",
               "bool not(if (bool isnull((int64) col1)) { (const bool) null } else { (int64) col1 IN (126, 123) })"}));
}

#if 0
TEST_F(FilterTestAnyAllOperators, Float48) {
  auto column = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2.125, 2.25, 2.5, 3});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "float4"}});
  ProcessWithFilter("col1", "col1 = any (array[2.125, 3]::float8[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[2.125,3.0]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.125"}, {"3"}}))
                        .SetGandivaFilters({"if (bool isnull((float) col1)) { (const bool) null } else { double "
                                            "castFLOAT8((float) col1) IN (2.125, 3) }",
                                            "if (bool isnull((float) col1)) { (const bool) null } else { double "
                                            "castFLOAT8((float) col1) IN (3, 2.125) }"}));
  ProcessWithFilter("col1", "col1 != all (array[2.125, 3]::float8[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-in\",\"term\":"
                                            "\"col1\",\"values\":[2.125,3.0]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.25"}, {"2.5"}}))
                        .SetGandivaFilters({"bool not(if (bool isnull((float) col1)) { (const bool) null } else { "
                                            "double castFLOAT8((float) col1) IN (2.125, 3) })",
                                            "bool not(if (bool isnull((float) col1)) { (const bool) null } else { "
                                            "double castFLOAT8((float) col1) IN (3, 2.125) })"}));
}

TEST_F(FilterTestAnyAllOperators, Float84) {
  auto column = MakeDoubleColumn("col1", 1, OptionalVector<double>{std::nullopt, 2.125, 2.25, 2.5, 3});
  PrepareData({column}, {GreenplumColumnInfo{.name = "col1", .type = "float8"}});
  ProcessWithFilter("col1", "col1 = any (array[2.125, 3]::float4[])",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"in\",\"term\":\"col1\",\"values\":[2.125,3.0]}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2.125"}, {"3"}}))
                        .SetGandivaFilters({"if (bool isnull((double) col1)) { (const bool) "
                                            "null } else { (double) col1 IN (2.125, 3) }",
                                            "if (bool isnull((double) col1)) { (const bool) "
                                            "null } else { (double) col1 IN (3, 2.125) }"}));
  ProcessWithFilter("col1", "col1 != all (array[2.125, 3]::float4[])",
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

}  // namespace
}  // namespace tea
