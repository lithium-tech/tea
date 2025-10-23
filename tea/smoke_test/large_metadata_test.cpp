#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/common.h"
#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class LargeMetadataTest : public TeaTest {};

TEST_F(LargeMetadataTest, Trivial) {
  constexpr int32_t kFilesNumber = (1 << 14);

  std::vector<FilePath> data_file_paths;

  std::string file_name_suffix_part(200, 'a');

  // teapot response size is ~5 * 10^6 bytes
  for (int i = 0; i < kFilesNumber; ++i) {
    std::string file_name_suffix_total = file_name_suffix_part + std::to_string(i) + ".parquet";
    auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{i});
    ASSIGN_OR_FAIL(auto file_path,
                   state_->WriteFile({column1}, IFileWriter::Hints{.row_group_sizes = {},
                                                                   .desired_file_suffix = file_name_suffix_total}));
    data_file_paths.emplace_back(file_path);
  }

  ASSERT_OK(state_->AddDataFiles({data_file_paths}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName + " LIMIT 1").Run(*this->conn_));
}

TEST_F(LargeMetadataTest, CancelTest) {
  if (Environment::GetTableType() != TestTableType::kExternal ||
      Environment::GetMetadataType() != MetadataType::kIceberg || Environment::GetProfile() != "samovar") {
    GTEST_SKIP();
    return;
  }

  constexpr int32_t kFilesNumber = (1 << 15);

  std::vector<FilePath> data_file_paths;

  std::string file_name_suffix_part(20, 'a');
  for (int j = 0; j < 4; ++j) {
    for (int i = 0; i < kFilesNumber / 4; ++i) {
      std::string file_name_suffix_total = file_name_suffix_part + std::to_string(i) + ".parquet";
      auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{i});
      ASSIGN_OR_FAIL(auto file_path,
                     state_->WriteFile({column1}, IFileWriter::Hints{.row_group_sizes = {},
                                                                     .desired_file_suffix = file_name_suffix_total}));
      data_file_paths.emplace_back(file_path);
    }

    ASSERT_OK(state_->AddDataFiles({data_file_paths}));
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  {
    auto query_start = std::chrono::steady_clock::now().time_since_epoch();

    auto async_select_query = pq::AsyncQuery("SELECT * FROM " + kDefaultTableName);
    EXPECT_TRUE(async_select_query.Run(*this->conn_));

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    async_select_query.CancelQuery(*this->conn_);

    ASSERT_TRUE(pq::Query("SELECT 1").Run(*conn_).ok());

    auto query_end = std::chrono::steady_clock::now().time_since_epoch();

    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start).count();
    EXPECT_LE(total_duration, 3500);
  }
}

}  // namespace
}  // namespace tea
