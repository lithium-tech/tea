#include <arrow/status.h>

#include <string>

#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/stats_state.h"
#include "tea/smoke_test/teapot_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class RealParquetTest : public TeaTest {};

TEST_F(RealParquetTest, FileOffsetInRowGroupIsNotSet) {
  if (Environment::GetMetadataType() != MetadataType::kTeapot) {
    GTEST_SKIP() << "Skip test for teapot";
  }
  ASSERT_OK(state_->AddDataFiles({"s3://warehouse/parquet/no_row_group_file_offset.parquet"}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "value", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "processed_dttm", .type = "timestamp"}}));

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName, "*").Run(*conn_));
  pq::ScanResult expected_result({"value", "processed_dttm"}, {{"1", "1970-01-01 00:00:00"}});

  EXPECT_EQ(result, expected_result);

  auto stats = stats_state_->GetStats(true);
  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    if (stat.data().data_files_read() > 0) {
      EXPECT_EQ(stat.data().data_files_read(), 1);

      auto s3_requests = stat.s3().s3_requests();
      auto s3_bytes = stat.s3().bytes_read_from_s3();

      auto read_duration = StatsState::DurationToNanos(stat.durations().read());

      EXPECT_GT(read_duration, 0);
      EXPECT_EQ(s3_requests, 2);
      EXPECT_EQ(s3_bytes, 618);
    }
  });
}

}  // namespace
}  // namespace tea
