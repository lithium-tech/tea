#include "tea/samovar/utils.h"

#include <iceberg/tea_scan.h>

#include "gtest/gtest.h"

namespace tea::samovar {

namespace {

struct Fragment {
  int64_t offset;
  int64_t length;
};

iceberg::ice_tea::DataEntry GetDataEntry(const std::vector<Fragment>& segments) {
  std::vector<iceberg::ice_tea::DataEntry::Segment> x;
  for (auto segment : segments) {
    x.emplace_back(segment.offset, segment.length);
  }
  return iceberg::ice_tea::DataEntry("", std::move(x));
}

void CheckEquals(const std::vector<iceberg::ice_tea::DataEntry>& entry, const std::vector<Fragment>& segments) {
  EXPECT_EQ(entry.size(), segments.size());

  for (size_t i = 0; i < segments.size(); ++i) {
    EXPECT_EQ(entry[i].parts.size(), 1);
    EXPECT_EQ(entry[i].parts[0].offset, segments[i].offset);
    EXPECT_EQ(entry[i].parts[0].length, segments[i].length);
  }
}

void TestCase(const std::vector<Fragment>& result_segments, const std::vector<Fragment>& initial_segments,
              const std::vector<int64_t>& row_group_offsets) {
  auto split_segments = SplitFileBySplitOffsets(GetDataEntry(initial_segments), row_group_offsets);
  CheckEquals(split_segments, result_segments);
}

TEST(SplitTasks, Test1) {
  TestCase({}, {}, {4});
  TestCase({{4, 0}}, {{4, 0}}, {});
  TestCase({{4, 99}}, {{4, 99}}, {});
  TestCase({{4, 99}}, {{4, 99}}, {4});
  TestCase({{4, 99}}, {{4, 99}}, {123});
  TestCase({{4, 36}, {40, 63}}, {{4, 99}}, {40});
  TestCase({{4, 1}, {5, 0}}, {{4, 0}}, {5});
  TestCase({{4, 1}, {5, 3}, {8, 0}}, {{4, 0}}, {5, 8});
  TestCase({{4, 1}, {5, 3}, {8, 8}}, {{4, 12}}, {5, 8});
  TestCase({{4, 1}, {5, 3}, {8, 1}, {12, 5}}, {{4, 5}, {12, 5}}, {5, 8});
  TestCase({{4, 40'000'000'000 - 4}, {40'000'000'000, 59'000'000'000 + 4}}, {{4, 99'000'000'000}}, {40'000'000'000});
}

}  // namespace
}  // namespace tea::samovar
