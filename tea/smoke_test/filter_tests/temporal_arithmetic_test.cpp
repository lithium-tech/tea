#include "tea/smoke_test/filter_tests/filter_test_base.h"
#include "tea/smoke_test/pq.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class FilterTestTemporalArithmetic : public FilterTestBase {};

TEST_F(FilterTestTemporalArithmetic, Date) {
  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{0, 1, 2, 3, 4, 5});
  ASSERT_OK(pq::SetTimeZone(*conn_, 1));
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "date"}});
  ProcessWithFilter("col1", "col1 - '1970-01-02'::date > 3",
                    ExpectedValues()
                        .SetGandivaFilters({"bool greater_than(int32 DateDiff((date32[day]) col1, date32[day] "
                                            "castDate((const int32) 1)), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-06"}})));
}

TEST_F(FilterTestTemporalArithmetic, TimestampInterval) {
  auto column1 =
      MakeTimestampColumn("col1", 1,
                          OptionalVector<int64_t>{86400000000 - 1, 86400000000, 86400000000 + 1, 2 * 86400000000 - 1,
                                                  2 * 86400000000, 2 * 86400000000 + 1});
  // ASSERT_OK(pq::SetTimeZone(*conn_, 1));
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  ProcessWithFilter(
      "col1", "col1 + interval '1 day' > '1970-01-03'::date",
      ExpectedValues()
          .SetGandivaFilters({"bool greater_than(timestamp[us] AddOverflow((timestamp[us]) col1, (const int64) "
                              "86400000000), date32[day] castDate((const int32) 2))"})
          .SetSelectResult(pq::ScanResult({"col1"}, {{"1970-01-02 00:00:00.000001"},
                                                     {"1970-01-02 23:59:59.999999"},
                                                     {"1970-01-03 00:00:00"},
                                                     {"1970-01-03 00:00:00.000001"}})));
}

TEST_F(FilterTestTemporalArithmetic, TimestampIntervalMonth) {
  auto column1 =
      MakeTimestampColumn("col1", 1,
                          OptionalVector<int64_t>{86400000000 - 1, 86400000000, 86400000000 + 1, 2 * 86400000000 - 1,
                                                  2 * 86400000000, 2 * 86400000000 + 1});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "timestamp"}});
  // TODO(gmusya): support filter for month
  ProcessWithFilter("col1", "col1 + interval '1 month' > '1970-01-03'::date::timestamp + '30 days'",
                    ExpectedValues().SetGandivaFilters({""}).SetSelectResult(
                        pq::ScanResult({"col1"}, {{"1970-01-02 00:00:00.000001"},
                                                  {"1970-01-02 23:59:59.999999"},
                                                  {"1970-01-03 00:00:00"},
                                                  {"1970-01-03 00:00:00.000001"}})));
}

}  // namespace
}  // namespace tea
