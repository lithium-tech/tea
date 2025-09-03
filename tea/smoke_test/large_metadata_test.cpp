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
  if (Environment::GetProfile() == "samovar") {
    // TODO(gmusya): fix redis parameters
    GTEST_SKIP();
  }
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

}  // namespace
}  // namespace tea
