#include <chrono>
#include <string>
#include <thread>

#include "gtest/gtest.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class CancelTest : public TeaTest {};

TEST_F(CancelTest, Select) {
  unsigned int seed = 42;

  OptionalVector<int32_t> values;
  for (int i = 0; i < 20000000; ++i) {
    values.push_back(i);
  }
  auto column1 = MakeInt32Column("col1", 1, values);
  auto column2 = MakeInt32Column("col2", 2, values);
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  auto async_select_query = pq::AsyncTableScanQuery(kDefaultTableName);
  EXPECT_TRUE(async_select_query.Run(*this->conn_));

  std::this_thread::sleep_for(std::chrono::milliseconds(rand_r(&seed) % 2000));
  auto start = std::chrono::high_resolution_clock::now();
  async_select_query.CancelQuery(*this->conn_);
  auto end = std::chrono::high_resolution_clock::now();

  std::chrono::duration<double> duration = end - start;
  EXPECT_LE(duration.count(), 0.05);
}

}  // namespace
}  // namespace tea
