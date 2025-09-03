#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/optional_vector.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/filter_tests/filter_test_base.h"
#include "tea/smoke_test/pq.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class IsNullTest : public FilterTestBase {};

TEST_F(IsNullTest, Boolean) {
  auto column1 = MakeBoolColumn("col1", 1, OptionalVector<bool>{std::nullopt, false, std::nullopt, true});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "bool"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((bool) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((bool) col1)", "bool not(bool isnull((bool) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Int2) {
  auto column1 = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int2"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((int16) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((int16) col1)", "bool not(bool isnull((int16) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Int4) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((int32) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((int32) col1)", "bool not(bool isnull((int32) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Int8) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((int64) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((int64) col1)", "bool not(bool isnull((int64) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

#if 0
TEST_F(IsNullTest, Float4) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "float4"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((float) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((float) col1)", "bool not(bool isnull((float) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Float8) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "float8"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((double) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((double) col1)", "bool not(bool isnull((double) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}
#endif

TEST_F(IsNullTest, String) {
  std::string str = "asd";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str, nullptr, &str});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});

  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "text"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}}))
                        .SetGandivaFilters({"bool isnull((string) col1)"}));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((string) col1)", "bool not(bool isnull((string) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Date) {
  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "date"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((date32[day]) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter(
      "col2", "col1 is not null",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                              "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
          .SetGandivaFilters({"bool isnotnull((date32[day]) col1)", "bool not(bool isnull((date32[day]) col1))"})
          .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Numeric) {
  auto column1 = MakeNumericColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 0, std::nullopt, 1}, 7, 2);
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((decimal128(7, 2)) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter("col2", "col1 is not null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                                            "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
                        .SetGandivaFilters({"bool isnotnull((decimal128(7, 2)) col1)",
                                            "bool not(bool isnull((decimal128(7, 2)) col1))"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Timestamp) {
  auto column1 = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});

  ProcessWithFilter("col2", "col1 is null",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
                        .SetGandivaFilters({"bool isnull((timestamp[us]) col1)"})
                        .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter(
      "col2", "col1 is not null",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                              "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
          .SetGandivaFilters({"bool isnotnull((timestamp[us]) col1)", "bool not(bool isnull((timestamp[us]) col1))"})
          .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, Timestamptz) {
  auto column1 = MakeTimestamptzColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 0, std::nullopt, 1});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2}, {GreenplumColumnInfo{.name = "col1", .type = "timestamptz"},
                                   GreenplumColumnInfo{.name = "col2", .type = "int4"}});
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ProcessWithFilter(
      "col2", "col1 is null",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"is-null\",\"term\":\"col1\"}"})
          .SetGandivaFilters(
              {"bool isnull(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) 25200000000))"})
          .SetSelectResult(pq::ScanResult({"col2"}, {{"1"}, {"3"}})));
  ProcessWithFilter(
      "col2", "col1 is not null",
      ExpectedValues()
          .SetIcebergFilters({"{\"type\":\"not-null\",\"term\":\"col1\"}",
                              "{\"type\":\"not\",\"child\":{\"type\":\"is-null\",\"term\":\"col1\"}}"})
          .SetGandivaFilters(
              {"bool isnotnull(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) 25200000000))",
               "bool not(bool isnull(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
               "25200000000)))"})
          .SetSelectResult(pq::ScanResult({"col2"}, {{"2"}, {"4"}})));
}

TEST_F(IsNullTest, EqualsNull) {
  std::string str = "str1";
  std::string empty_str = "";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str, nullptr, &empty_str});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{42, std::nullopt, 0});
  auto column3 = MakeFloatColumn("col3", 3, OptionalVector<float>{std::nullopt, 0, 42.0});
  PrepareData({column1, column2},
              {GreenplumColumnInfo{.name = "col1", .type = "text"}, GreenplumColumnInfo{.name = "col2", .type = "int4"},
               GreenplumColumnInfo{.name = "col3", .type = "float4"}});

  ProcessWithFilter("col2", "col1 != null", ExpectedValues().SetSelectResult(pq::ScanResult({"col2"}, {})));
  ProcessWithFilter("col2", "col2 != null", ExpectedValues().SetSelectResult(pq::ScanResult({"col2"}, {})));
  ProcessWithFilter("col2", "col3 != null", ExpectedValues().SetSelectResult(pq::ScanResult({"col2"}, {})));
}

}  // namespace
}  // namespace tea
