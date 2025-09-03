#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestTemporal : public FilterTestBase {};

#if 0
TEST_F(FilterTestTemporal, ExtractMillennium) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         -210866803200000000,    // "4714-11-24 00:00:00 BC"
                                         0,                      // "1970-01-01 00:00:00"
                                         86400000000ll * 10956,  // "1999-12-31 00:00:00"
                                         86400000000ll * 10957,  // "2000-01-01 00:00:00"
                                         86400000000ll * 11322,  // "2000-12-31 00:00:00"
                                         86400000000ll * 11323,  // "2001-01-01 00:00:00"
                                         86400000000ll * 27381,  // "2044-12-19 00:00:00"
                                         86400000000ll * 427381  // "3140-02-18 00:00:00"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Millennium FROM col1) = 3",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMillennium((timestamp[us]) col1), "
                                            "(const double) 3 raw(4008000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2001-01-01 00:00:00"}, {"2044-12-19 00:00:00"}})));
  ProcessWithFilter(
      "col1", "extract(Millennium FROM col1) = 2",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(double ExtractMillennium((timestamp[us]) col1), "
                              "(const double) 2 raw(4000000000000000))"})
          .SetSelectResult(pq::ScanResult(
              {"col1"},
              {{"1970-01-01 00:00:00"}, {"1999-12-31 00:00:00"}, {"2000-01-01 00:00:00"}, {"2000-12-31 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Millennium FROM col1) = -5",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMillennium((timestamp[us]) col1), "
                                            "(const double) -5 raw(c014000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"4714-11-24 00:00:00 BC"}})));
}

TEST_F(FilterTestTemporal, ExtractCentury) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         -210866803200000000,    // "4714-11-24 00:00:00 BC"
                                         0,                      // "1970-01-01 00:00:00"
                                         86400000000ll * 10956,  // "1999-12-31 00:00:00"
                                         86400000000ll * 10957,  // "2000-01-01 00:00:00"
                                         86400000000ll * 11322,  // "2000-12-31 00:00:00"
                                         86400000000ll * 11323,  // "2001-01-01 00:00:00"
                                         86400000000ll * 27381,  // "2044-12-19 00:00:00"
                                         86400000000ll * 427381  // "3140-02-18 00:00:00"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter(
      "col1", "extract(Century FROM col1) = 20",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(double ExtractCentury((timestamp[us]) col1), "
                              "(const double) 20 raw(4034000000000000))"})
          .SetSelectResult(pq::ScanResult(
              {"col1"},
              {{"1970-01-01 00:00:00"}, {"1999-12-31 00:00:00"}, {"2000-01-01 00:00:00"}, {"2000-12-31 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Century FROM col1) = 21",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractCentury((timestamp[us]) col1), "
                                            "(const double) 21 raw(4035000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2001-01-01 00:00:00"}, {"2044-12-19 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Century FROM col1) = -48",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractCentury((timestamp[us]) col1), "
                                            "(const double) -48 raw(c048000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"4714-11-24 00:00:00 BC"}})));
}

TEST_F(FilterTestTemporal, ExtractDecade) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         -210866803200000000,    // "4714-11-24 00:00:00 BC"
                                         0,                      // "1970-01-01 00:00:00"
                                         86400000000ll * 10956,  // "1999-12-31 00:00:00"
                                         86400000000ll * 10957,  // "2000-01-01 00:00:00"
                                         86400000000ll * 11322,  // "2000-12-31 00:00:00"
                                         86400000000ll * 11323,  // "2001-01-01 00:00:00"
                                         86400000000ll * 27381,  // "2044-12-19 00:00:00"
                                         86400000000ll * 427381  // "3140-02-18 00:00:00"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Decade FROM col1) = 200",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractDecade((timestamp[us]) col1), (const "
                                            "double) 200 raw(4069000000000000))"})
                        .SetSelectResult(pq::ScanResult(
                            {"col1"}, {{"2000-01-01 00:00:00"}, {"2000-12-31 00:00:00"}, {"2001-01-01 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Decade FROM col1) = 199",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractDecade((timestamp[us]) col1), (const "
                                            "double) 199 raw(4068e00000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1999-12-31 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Decade FROM col1) = -472",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractDecade((timestamp[us]) col1), (const "
                                            "double) -472 raw(c07d800000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"4714-11-24 00:00:00 BC"}})));
}

TEST_F(FilterTestTemporal, ExtractYear) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         -210866803200000000,    // "4714-11-24 00:00:00 BC"
                                         0,                      // "1970-01-01 00:00:00"
                                         86400000000ll * 10956,  // "1999-12-31 00:00:00"
                                         86400000000ll * 10957,  // "2000-01-01 00:00:00"
                                         86400000000ll * 11322,  // "2000-12-31 00:00:00"
                                         86400000000ll * 11323,  // "2001-01-01 00:00:00"
                                         86400000000ll * 27381,  // "2044-12-19 00:00:00"
                                         86400000000ll * 427381  // "3140-02-18 00:00:00"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Year FROM col1) = 2000",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractYear((timestamp[us]) col1), (const "
                                            "double) 2000 raw(409f400000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2000-01-01 00:00:00"}, {"2000-12-31 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Year FROM col1) = 2001",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractYear((timestamp[us]) col1), (const "
                                            "double) 2001 raw(409f440000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"2001-01-01 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Year FROM col1) = -4714",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractYear((timestamp[us]) col1), (const "
                                            "double) -4714 raw(c0b26a0000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"4714-11-24 00:00:00 BC"}})));
}

TEST_F(FilterTestTemporal, ExctractQuarter) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         -210866803200000000,  // "4714-11-24 00:00:00 BC"
                                         0,                    // "1970-01-01 00:00:00"
                                         86400000000ll * 89,   // "1970-03-31 00:00:00"
                                         86400000000ll * 90,   // "1970-04-01 00:00:00"
                                         86400000000ll * 180,  // "1970-06-30 00:00:00"
                                         86400000000ll * 181,  // "1970-07-01 00:00:00"
                                         86400000000ll * 272,  // "1970-09-30 00:00:00"
                                         86400000000ll * 273,  // "1970-10-01 00:00:00"
                                         86400000000ll * 364,  // "1970-12-31 00:00:00"
                                         86400000000ll * 365,  // "1971-01-01 00:00:00"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Quarter FROM col1) = 1",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractQuarter((timestamp[us]) col1), "
                                            "(const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult(
                            {"col1"}, {{"1970-01-01 00:00:00"}, {"1970-03-31 00:00:00"}, {"1971-01-01 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Quarter FROM col1) = 2",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractQuarter((timestamp[us]) col1), "
                                            "(const double) 2 raw(4000000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-04-01 00:00:00"}, {"1970-06-30 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Quarter FROM col1) = 3",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractQuarter((timestamp[us]) col1), "
                                            "(const double) 3 raw(4008000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-07-01 00:00:00"}, {"1970-09-30 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Quarter FROM col1) = 4",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractQuarter((timestamp[us]) col1), "
                                            "(const double) 4 raw(4010000000000000))"})
                        .SetSelectResult(pq::ScanResult(
                            {"col1"}, {{"4714-11-24 00:00:00 BC"}, {"1970-10-01 00:00:00"}, {"1970-12-31 00:00:00"}})));
}

TEST_F(FilterTestTemporal, ExtractMonth) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         -210866803200000000,  // "4714-11-24 00:00:00 BC"
                                         0,                    // "1970-01-01 00:00:00"
                                         86400000000ll * 90,   // "1970-04-01 00:00:00"
                                         86400000000ll * 364,  // "1970-12-31 00:00:00"
                                         86400000000ll * 365,  // "1971-01-01 00:00:00"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Month FROM col1) = 1",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMonth((timestamp[us]) "
                                            "col1), (const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:00"}, {"1971-01-01 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Month FROM col1) = 11",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMonth((timestamp[us]) col1), (const "
                                            "double) 11 raw(4026000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"4714-11-24 00:00:00 BC"}})));
}

TEST_F(FilterTestTemporal, ExtractWeek) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         86400000000ll * (-365),  // "1969-01-01 00:00:00" wednesday
                                         -1,                      // "1969-12-31 23:59:59.999999" wednesday
                                         0,                       // "1970-01-01 00:00:00" thursday
                                         86400000000ll * 3,       // "1970-01-04 00:00:00" sunday
                                         86400000000ll * 4,       // "1970-01-05 00:00:00" monday
                                         86400000000ll * 365,     // "1971-01-01 00:00:00" friday
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Week FROM col1) = 1",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractWeek((timestamp[us]) "
                                            "col1), (const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1969-01-01 00:00:00"},
                                                                   {"1969-12-31 23:59:59.999999"},
                                                                   {"1970-01-01 00:00:00"},
                                                                   {"1970-01-04 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Week FROM col1) = 2",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractWeek((timestamp[us]) "
                                            "col1), (const double) 2 raw(4000000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-05 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Week FROM col1) = 53",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractWeek((timestamp[us]) "
                                            "col1), (const double) 53 raw(404a800000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1971-01-01 00:00:00"}})));
}

TEST_F(FilterTestTemporal, ExtractDay) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         86400000000ll * (-365) - 1,  // "1968-12-31 23:59:59.999999"
                                         86400000000ll * (-365),      // "1969-01-01 00:00:00"
                                         -1,                          // "1969-12-31 23:59:59.999999"
                                         0,                           // "1970-01-01 00:00:00"
                                         86400000000ll * 365,         // "1971-01-01 00:00:00"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Day FROM col1) = 1",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractDay((timestamp[us]) "
                                            "col1), (const double) 1 raw(3ff0000000000000))"})
                        .SetSelectResult(pq::ScanResult(
                            {"col1"}, {{"1969-01-01 00:00:00"}, {"1970-01-01 00:00:00"}, {"1971-01-01 00:00:00"}})));
  ProcessWithFilter(
      "col1", "extract(Day FROM col1) = 31",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(double ExtractDay((timestamp[us]) "
                              "col1), (const double) 31 raw(403f000000000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1968-12-31 23:59:59.999999"}, {"1969-12-31 23:59:59.999999"}})));
}

TEST_F(FilterTestTemporal, ExtractHour) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         0,                  // "1970-01-01 00:00:00"
                                         3600000000ll,       // "1970-01-01 01:00:00
                                         3600000000ll - 1,   // "1970-01-01 00:59:59.999999"
                                         3600000000ll + 1,   // "1970-01-01 01:00:00.000001"
                                         86400000000ll,      // "1970-01-02 00:00:00"
                                         86400000000ll - 1,  // "1970-01-01 23:59:59.999999"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter(
      "col1", "extract(Hour FROM col1) = 0",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(double ExtractHour((timestamp[us]) "
                              "col1), (const double) 0 raw(0))"})
          .SetSelectResult(pq::ScanResult(
              {"col1"}, {{"1970-01-01 00:00:00"}, {"1970-01-01 00:59:59.999999"}, {"1970-01-02 00:00:00"}})));
  ProcessWithFilter(
      "col1", "extract(Hour FROM col1) = 1",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(double ExtractHour((timestamp[us]) "
                              "col1), (const double) 1 raw(3ff0000000000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 01:00:00"}, {"1970-01-01 01:00:00.000001"}})));
}

TEST_F(FilterTestTemporal, ExtractMinute) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         0,                   // "1970-01-01 00:00:00"
                                         60000000ll,          // "1970-01-01 00:01:00
                                         60000000ll - 1,      // "1970-01-01 00:00:59.999999"
                                         60000000ll + 1,      // "1970-01-01 00:01:00.000001"
                                         2 * 60000000ll,      // "1970-01-01 00:02:00"
                                         2 * 60000000ll - 1,  // "1970-01-01 00:01:59.999999"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter(
      "col1", "extract(Minute FROM col1) = 0",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(double ExtractMinute((timestamp[us]) "
                              "col1), (const double) 0 raw(0))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:00"}, {"1970-01-01 00:00:59.999999"}})));
  ProcessWithFilter(
      "col1", "extract(Minute FROM col1) = 1",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(double ExtractMinute((timestamp[us]) "
                              "col1), (const double) 1 raw(3ff0000000000000))"})
          .SetSelectResult(pq::ScanResult(
              {"col1"}, {{"1970-01-01 00:01:00"}, {"1970-01-01 00:01:00.000001"}, {"1970-01-01 00:01:59.999999"}})));
}

// Extract second returns seconds + micros / 1000 + millis / 1000000
// https://www.postgresql.org/docs/9.4/functions-datetime.html
TEST_F(FilterTestTemporal, ExtractSecond) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         0,                  // "1970-01-01 00:00:00"
                                         1000000ll,          // "1970-01-01 00:00:01
                                         1000000ll - 1,      // "1970-01-01 00:00:00.999999"
                                         1000000ll + 1,      // "1970-01-01 00:00:00.000001"
                                         2 * 1000000ll,      // "1970-01-01 00:00:02"
                                         2 * 1000000ll - 1,  // "1970-01-01 00:00:01.999999"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Second FROM col1) = 0.999999",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractSecond((timestamp[us]) col1), (const "
                                            "double) 0.999999 raw(3feffffde7210be9))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:00.999999"}})));
  ProcessWithFilter("col1", "extract(Second FROM col1) = 2",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractSecond((timestamp[us]) "
                                            "col1), (const double) 2 raw(4000000000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:02"}})));
}

TEST_F(FilterTestTemporal, ExtractMilliseconds) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         0,                  // "1970-01-01 00:00:00"
                                         1000000ll,          // "1970-01-01 00:00:01
                                         1000000ll - 1,      // "1970-01-01 00:00:00.999999"
                                         1000000ll + 1,      // "1970-01-01 00:00:00.000001"
                                         2 * 1000000ll,      // "1970-01-01 00:00:02"
                                         2 * 1000000ll - 1,  // "1970-01-01 00:00:01.999999"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Milliseconds FROM col1) = 0",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMilliseconds((timestamp[us]) col1), "
                                            "(const double) 0 raw(0))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Milliseconds FROM col1) = 1000",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMilliseconds((timestamp[us]) col1), "
                                            "(const double) 1000 raw(408f400000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:01"}})));
}

TEST_F(FilterTestTemporal, ExtractMicroseconds) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         0,                  // "1970-01-01 00:00:00"
                                         1000000ll,          // "1970-01-01 00:00:01
                                         1000000ll - 1,      // "1970-01-01 00:00:00.999999"
                                         1000000ll + 1,      // "1970-01-01 00:00:00.000001"
                                         2 * 1000000ll,      // "1970-01-01 00:00:02"
                                         2 * 1000000ll - 1,  // "1970-01-01 00:00:01.999999"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter("col1", "extract(Microseconds FROM col1) = 0",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMicroseconds((timestamp[us]) col1), "
                                            "(const double) 0 raw(0))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:00"}})));
  ProcessWithFilter("col1", "extract(Microseconds FROM col1) = 999999",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(double ExtractMicroseconds((timestamp[us]) col1), "
                                            "(const double) 999999 raw(412e847e00000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-01 00:00:00.999999"}})));
}

TEST_F(FilterTestTemporal, DateTrunc) {
  auto column1 = MakeTimestampColumn("col1", 1,
                                     OptionalVector<int64_t>{
                                         -86400000000ll * 1000 + 7023456789,  // "1967-04-07 01:57:03.456789"
                                         0,                                   // "1970-01-01 00:00:00"
                                         86400000000ll * 27381 + 987654321    // "2044-12-19 00:16:27.654321"
                                     });
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter(
      "col1", "date_trunc('Millennium', col1) = '1001-01-01 00:00:00'",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(timestamp[us] DateTruncMillennium((timestamp[us]) "
                              "col1), timestamp[us] castTIMESTAMP((const int64) "
                              "-30578688000000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}, {"1970-01-01 00:00:00"}})));
  ProcessWithFilter(
      "col1", "date_trunc('Century', col1) = '1901-01-01 00:00:00'",
      ExpectedValues()
          .SetGandivaFilters({"bool equal(timestamp[us] DateTruncCentury((timestamp[us]) "
                              "col1), timestamp[us] castTIMESTAMP((const int64) "
                              "-2177452800000000))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}, {"1970-01-01 00:00:00"}})));
  ProcessWithFilter("col1", "date_trunc('Decade', col1) = '1960-01-01 00:00:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncDecade((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-315619200000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Year', col1) = '1967-01-01 00:00:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncYear((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-94694400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Quarter', col1) = '1967-04-01 00:00:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncQuarter((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-86918400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Month', col1) = '1967-04-01 00:00:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncMonth((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-86918400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Week', col1) = '1967-04-03 00:00:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncWeek((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-86745600000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Day', col1) = '1967-04-07 00:00:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncDay((timestamp[us]) col1), "
                                            "timestamp[us] castTIMESTAMP((const int64) -86400000000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Hour', col1) = '1967-04-07 01:00:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncHour((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-86396400000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Minute', col1) = '1967-04-07 01:57:00'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncMinute((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-86392980000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Second', col1) = '1967-04-07 01:57:03'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] DateTruncSecond((timestamp[us]) "
                                            "col1), timestamp[us] castTIMESTAMP((const int64) "
                                            "-86392977000000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
  ProcessWithFilter("col1", "date_trunc('Milliseconds', col1) = '1967-04-07 01:57:03.456000'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(timestamp[us] "
                                            "DateTruncMilliseconds((timestamp[us]) col1), timestamp[us] "
                                            "castTIMESTAMP((const int64) -86392976544000))"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1967-04-07 01:57:03.456789"}})));
}
#endif

}  // namespace
}  // namespace tea
