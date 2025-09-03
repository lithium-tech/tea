#include <iceberg/test_utils/column.h>

#include "tea/smoke_test/filter_tests/filter_test_base.h"
#include "tea/test_utils/common.h"

namespace tea {
namespace {

class FilterTestCoalesce : public FilterTestBase {};

TEST_F(FilterTestCoalesce, Simple) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 5, std::nullopt});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{0, std::nullopt, 1, std::nullopt});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{1, 2, 3, 4});
  auto column4 = MakeInt32Column("col4", 4, OptionalVector<int32_t>{0, 0, std::nullopt, 15});
  PrepareData(
      {column1, column2, column3, column4},
      {GreenplumColumnInfo{.name = "col1", .type = "int4"}, GreenplumColumnInfo{.name = "col2", .type = "int4"},
       GreenplumColumnInfo{.name = "col3", .type = "int4"}, GreenplumColumnInfo{.name = "col4", .type = "int4"}});
  ProcessWithFilter("col3", "coalesce(col1, col2) > 2",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetGandivaFilters({"bool greater_than(if (bool isnull((int32) col1)) { (int32) col2 } else { "
                                            "(int32) col1 }, (const int32) 2)"})
                        .SetSelectResult(pq::ScanResult({"col3"}, {{"3"}})));

  auto stats = stats_state_->GetStats(false);
  for (const auto& stat : stats) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 3);
    }
  }
}

TEST_F(FilterTestCoalesce, TypePromotion) {
  auto column1 = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 5, std::nullopt});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{0, std::nullopt, 1, std::nullopt});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{1, 2, 3, 4});
  auto column4 = MakeInt32Column("col4", 4, OptionalVector<int32_t>{0, 0, std::nullopt, 15});
  PrepareData(
      {column1, column2, column3, column4},
      {GreenplumColumnInfo{.name = "col1", .type = "int2"}, GreenplumColumnInfo{.name = "col2", .type = "int8"},
       GreenplumColumnInfo{.name = "col3", .type = "int4"}, GreenplumColumnInfo{.name = "col4", .type = "int4"}});
  ProcessWithFilter(
      "col3", "coalesce(col1, col2, col3) > 2",
      ExpectedValues()
          .SetIcebergFilters({""})
          .SetGandivaFilters({"bool greater_than(if (bool isnull(int64 castBIGINT((int16) col1))) { if (bool "
                              "isnull((int64) col2)) { int64 castBIGINT((int32) col3) } else { (int64) col2 } } else { "
                              "int64 castBIGINT((int16) col1) }, int64 castBIGINT((const int32) 2))"})
          .SetSelectResult(pq::ScanResult({"col3"}, {{"3"}, {"4"}})));

  auto stats = stats_state_->GetStats(false);
  for (const auto& stat : stats) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 3);
    }
  }
}

TEST_F(FilterTestCoalesce, OneArgument) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 5, std::nullopt});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{0, std::nullopt, 1, std::nullopt});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{1, 2, 3, 4});
  auto column4 = MakeInt32Column("col4", 4, OptionalVector<int32_t>{0, 0, std::nullopt, 15});
  PrepareData(
      {column1, column2, column3, column4},
      {GreenplumColumnInfo{.name = "col1", .type = "int4"}, GreenplumColumnInfo{.name = "col2", .type = "int8"},
       GreenplumColumnInfo{.name = "col3", .type = "int4"}, GreenplumColumnInfo{.name = "col4", .type = "int4"}});
  ProcessWithFilter("col3", "coalesce(col1) > 2",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"gt\",\"term\":\"col1\",\"value\":2}"})
                        .SetGandivaFilters({"bool greater_than((int32) col1, (const int32) 2)"})
                        .SetSelectResult(pq::ScanResult({"col3"}, {{"3"}})));

  auto stats = stats_state_->GetStats(false);
  for (const auto& stat : stats) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 2);
    }
  }
}

TEST_F(FilterTestCoalesce, MultipleArguments) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 5, std::nullopt});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{0, std::nullopt, 1, std::nullopt});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{1, 2, 3, 4});
  auto column4 = MakeInt32Column("col4", 4, OptionalVector<int32_t>{0, 0, std::nullopt, 15});
  PrepareData(
      {column1, column2, column3, column4},
      {GreenplumColumnInfo{.name = "col1", .type = "int4"}, GreenplumColumnInfo{.name = "col2", .type = "int8"},
       GreenplumColumnInfo{.name = "col3", .type = "int4"}, GreenplumColumnInfo{.name = "col4", .type = "int4"}});
  ProcessWithFilter(
      "col3", "coalesce(col1, col2, 15) > 2",
      ExpectedValues()
          .SetIcebergFilters({""})
          .SetGandivaFilters({"bool greater_than(if (bool isnull(int64 castBIGINT((int32) col1))) { if (bool "
                              "isnull((int64) col2)) { (const int64) 15 } else { (int64) col2 } } else { int64 "
                              "castBIGINT((int32) col1) }, int64 castBIGINT((const int32) 2))"})
          .SetSelectResult(pq::ScanResult({"col3"}, {{"3"}, {"4"}})));

  auto stats = stats_state_->GetStats(false);
  for (const auto& stat : stats) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 3);
    }
  }
}

TEST_F(FilterTestCoalesce, FunctionAsArgument) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 5, std::nullopt});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{0, std::nullopt, 1, std::nullopt});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{1, 2, 3, 4});
  auto column4 = MakeInt32Column("col4", 4, OptionalVector<int32_t>{0, 0, std::nullopt, 15});
  PrepareData(
      {column1, column2, column3, column4},
      {GreenplumColumnInfo{.name = "col1", .type = "int4"}, GreenplumColumnInfo{.name = "col2", .type = "int8"},
       GreenplumColumnInfo{.name = "col3", .type = "int4"}, GreenplumColumnInfo{.name = "col4", .type = "int4"}});
  ProcessWithFilter(
      "col3", "coalesce(col1, col2 + col3, 15) > 2",
      ExpectedValues()
          .SetIcebergFilters({""})
          .SetGandivaFilters({"bool greater_than(if (bool isnull(int64 castBIGINT((int32) col1))) { if (bool "
                              "isnull(int64 AddOverflow((int64) col2, int64 castBIGINT((int32) col3)))) { (const "
                              "int64) 15 } else { int64 AddOverflow((int64) col2, int64 castBIGINT((int32) col3)) } } "
                              "else { int64 castBIGINT((int32) col1) }, int64 castBIGINT((const int32) 2))"})
          .SetSelectResult(pq::ScanResult({"col3"}, {{"3"}, {"4"}})));
  auto stats = stats_state_->GetStats(false);
  for (const auto& stat : stats) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.projection().columns_read(), 3);
    }
  }
}

TEST_F(FilterTestCoalesce, Temporal) {
  ASSERT_OK(pq::SetTimeZone(*conn_, 2));

  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 5, std::nullopt});
  auto column2 = MakeTimestampColumn("col2", 2, OptionalVector<int64_t>{0, std::nullopt, 1, std::nullopt});
  auto column3 = MakeTimestamptzColumn("col3", 3, OptionalVector<int64_t>{1, 2, 3, 4});
  auto column4 = MakeInt32Column("col4", 4, OptionalVector<int32_t>{1, 2, 3, 4});
  PrepareData({column1, column2, column3, column4}, {GreenplumColumnInfo{.name = "col1", .type = "date"},
                                                     GreenplumColumnInfo{.name = "col2", .type = "timestamp"},
                                                     GreenplumColumnInfo{.name = "col3", .type = "timestamptz"},
                                                     GreenplumColumnInfo{.name = "col4", .type = "int4"}});
  // currently is not supported, becase casts "date -> timestamp" and "date -> timestamptz" are not supported
  ProcessWithFilter("col4", "coalesce(col1, col2, col3)::timestamp > '1970-01-02'::date",
                    ExpectedValues().SetIcebergFilters({""}).SetGandivaFilters({""}).SetSelectResult(
                        pq::ScanResult({"col3"}, {{"2"}, {"3"}})));
  auto stats = stats_state_->GetStats(false);
}

}  // namespace
}  // namespace tea
