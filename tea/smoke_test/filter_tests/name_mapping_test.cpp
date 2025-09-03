#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class NameMappingTest : public FilterTestBase {};

TEST_F(NameMappingTest, Simple) {
  auto column1 = MakeInt32Column("Col1", 1, OptionalVector<int32_t>{std::nullopt, 122, 123, 124});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "int4"}});
  ProcessWithFilter("Col1", "Col1 < 123",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"lt\",\"term\":\"col1\",\"value\":123}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"122"}}))
                        .SetGandivaFilters({"bool less_than((int32) Col1, (const int32) 123)"}));
}

}  // namespace
}  // namespace tea
