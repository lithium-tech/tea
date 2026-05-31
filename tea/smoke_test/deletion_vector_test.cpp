#include <string>
#include <vector>

#include "arrow/status.h"
#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/spark_generated_test_base.h"
#include "tea/smoke_test/test_base.h"

namespace tea {
namespace {

TEST_F(OtherEngineGeneratedTable, DeletionVectorSimpleScanAndMetrics) {
  CreateTable("test", "deletion_vectors",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "c1", .type = "int4"},
                                               GreenplumColumnInfo{.name = "c2", .type = "int4"}});

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::Query("SELECT c1, c2 FROM " + kDefaultTableName + " ORDER BY c1").Run(*conn_));
  EXPECT_EQ(result.values.size(), 8);

  int64_t rows_skipped_deletion_vector = 0;
  int64_t deletion_vectors_planned = 0;
  int64_t dangling_deletion_vector_files = 0;

  for (const auto& stat : stats_state_->GetStats(true)) {
    rows_skipped_deletion_vector += stat.data().rows_skipped_deletion_vector();
    deletion_vectors_planned += stat.plan().deletion_vectors_planned();
    dangling_deletion_vector_files += stat.plan().dangling_deletion_vector_files();
  }

  EXPECT_EQ(rows_skipped_deletion_vector, 8);
  EXPECT_GE(deletion_vectors_planned, 2);
  EXPECT_EQ(dangling_deletion_vector_files, 0);
}

}  // namespace
}  // namespace tea
