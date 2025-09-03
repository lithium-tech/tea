#include "tea/smoke_test/filter_tests/filter_test_base.h"
#include "tea/smoke_test/pq.h"

namespace tea {
namespace {

class FilterTestTemporalConvert : public FilterTestBase {};

TEST_F(FilterTestTemporalConvert, CastTimestamptzToDate) {
  auto column1 =
      MakeTimestamptzColumn("col1", 1,
                            OptionalVector<int64_t>{86400000000 - 3600000000 - 1, 86400000000 - 3600000000,
                                                    86400000000 - 3600000000 + 1, 2 * 86400000000 - 3600000000 - 1,
                                                    2 * 86400000000 - 3600000000, 2 * 86400000000 - 3600000000 + 1});
  ASSERT_OK(pq::SetTimeZone(*conn_, 1));
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}});
  ProcessWithFilter(
      "col1", "date(col1) > '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool greater_than(date32[day] castDATE(timestamp[us] castTIMESTAMP((timestamp[us, "
                              "tz=UTC]) col1, (const int64) 3600000000)), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-03 00:00:00+01"}, {"1970-01-03 00:00:00.000001+01"}})));
  ProcessWithFilter(
      "col1", "date(col1) = '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(date32[day] castDATE(timestamp[us] castTIMESTAMP((timestamp[us, "
                              "tz=UTC]) col1, (const int64) 3600000000)), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult(
              {"col1"},
              {{"1970-01-02 00:00:00+01"}, {"1970-01-02 00:00:00.000001+01"}, {"1970-01-02 23:59:59.999999+01"}})));
  ProcessWithFilter(
      "col1", "date(col1) < '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool less_than(date32[day] castDATE(timestamp[us] castTIMESTAMP((timestamp[us, "
                              "tz=UTC]) col1, (const int64) 3600000000)), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 23:59:59.999999+01"}})));
}

TEST_F(FilterTestTemporalConvert, CastTimestampToDate) {
  auto column1 =
      MakeTimestampColumn("col1", 1,
                          OptionalVector<int64_t>{86400000000 - 1, 86400000000, 86400000000 + 1, 2 * 86400000000 - 1,
                                                  2 * 86400000000, 2 * 86400000000 + 1});
  ASSERT_OK(pq::SetTimeZone(*conn_, 1));
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter(
      "col1", "date(col1) > '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters(
              {"bool greater_than(date32[day] castDATE((timestamp[us]) col1), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-03 00:00:00"}, {"1970-01-03 00:00:00.000001"}})));
  ProcessWithFilter(
      "col1", "date(col1) = '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters(
              {"bool equal(date32[day] castDATE((timestamp[us]) col1), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult(
              {"col1"}, {{"1970-01-02 00:00:00"}, {"1970-01-02 00:00:00.000001"}, {"1970-01-02 23:59:59.999999"}})));
  ProcessWithFilter(
      "col1", "date(col1) < '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters(
              {"bool less_than(date32[day] castDATE((timestamp[us]) col1), date32[day] castDate((const int32) 1))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 23:59:59.999999"}})));
}

TEST_F(FilterTestTemporalConvert, CastTimestamptzToTimestamp) {
  auto column1 =
      MakeTimestamptzColumn("col1", 1,
                            OptionalVector<int64_t>{86400000000 - 3600000000 - 1, 86400000000 - 3600000000,
                                                    86400000000 - 3600000000 + 1, 2 * 86400000000 - 3600000000 - 1,
                                                    2 * 86400000000 - 3600000000, 2 * 86400000000 - 3600000000 + 1});
  ASSERT_OK(pq::SetTimeZone(*conn_, 1));
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}});
  ProcessWithFilter(
      "col1::timestamp", "col1::timestamp > '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool greater_than(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const "
                              "int64) 3600000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-02 00:00:00.000001"},
                                                     {"1970-01-02 23:59:59.999999"},
                                                     {"1970-01-03 00:00:00"},
                                                     {"1970-01-03 00:00:00.000001"}})));
  ProcessWithFilter(
      "col1::timestamp", "col1::timestamp = '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
                              "3600000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-02 00:00:00"}})));
  ProcessWithFilter(
      "col1::timestamp", "col1::timestamp < '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool less_than(timestamp[us] castTIMESTAMP((timestamp[us, tz=UTC]) col1, (const int64) "
                              "3600000000), timestamp[us] castTIMESTAMP((const int64) 86400000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 23:59:59.999999"}})));
}

TEST_F(FilterTestTemporalConvert, CastTimestampToTimestamptz) {
  auto column1 =
      MakeTimestampColumn("col1", 1,
                          OptionalVector<int64_t>{86400000000 - 1, 86400000000, 86400000000 + 1, 2 * 86400000000 - 1,
                                                  2 * 86400000000, 2 * 86400000000 + 1});
  ASSERT_OK(pq::SetTimeZone(*conn_, 1));
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter(
      "col1::timestamptz", "col1::timestamptz > '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool greater_than(timestamp[us, tz=UTC] castTIMESTAMPTZ((timestamp[us]) col1, (const "
                              "int64) -3600000000), timestamp[us, tz=UTC] castTIMESTAMPTZ((const int64) 82800000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-02 00:00:00.000001+01"},
                                                     {"1970-01-02 23:59:59.999999+01"},
                                                     {"1970-01-03 00:00:00+01"},
                                                     {"1970-01-03 00:00:00.000001+01"}})));
  ProcessWithFilter(
      "col1::timestamptz", "col1::timestamptz = '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(timestamp[us, tz=UTC] castTIMESTAMPTZ((timestamp[us]) col1, (const int64) "
                              "-3600000000), timestamp[us, tz=UTC] castTIMESTAMPTZ((const int64) 82800000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-02 00:00:00+01"}})));
  ProcessWithFilter(
      "col1::timestamptz", "col1::timestamptz < '1970-01-02'",
      ExpectedValues()
          .SetGandivaFilters({"bool less_than(timestamp[us, tz=UTC] castTIMESTAMPTZ((timestamp[us]) col1, (const "
                              "int64) -3600000000), timestamp[us, tz=UTC] castTIMESTAMPTZ((const int64) 82800000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 23:59:59.999999+01"}})));
}

}  // namespace
}  // namespace tea
