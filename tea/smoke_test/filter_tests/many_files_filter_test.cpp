#include <random>

#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/filter_tests/filter_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class ManyFilesFilterTest : public FilterTestBase {};

TEST_F(ManyFilesFilterTest, Text2) {
  std::string str1 = "good1";
  std::string str2 = "bad1";
  std::string str3 = "good2";
  std::string str4 = "bad2";

  std::mt19937 rnd(2101);

  int match = 0;
  int skipped = 0;

  std::vector<FragmentInfo> frags;
  for (int file_num = 0; file_num < 10; ++file_num) {
    OptionalVector<int32_t> col_0_data;
    std::vector<std::string*> col_1_data;
    std::vector<std::string*> col_2_data;

    constexpr int64_t kRowsInFile = 10'000;
    for (int row_num = 0; row_num < kRowsInFile; ++row_num) {
      col_0_data.emplace_back(row_num + file_num * 100);
      if (rnd() % 4 == 0) {
        col_1_data.emplace_back(&str1);
        col_2_data.emplace_back(&str3);
        ++match;
      } else {
        col_1_data.emplace_back(&str2);
        col_2_data.emplace_back(&str4);
        ++skipped;
      }
    }

    auto column0 = MakeInt32Column("col0", 0, col_0_data);
    auto column1 = MakeStringColumn("col1", 1, col_1_data);
    auto column2 = MakeStringColumn("col2", 2, col_2_data);

    std::vector<size_t> row_group_sizes(10, kRowsInFile / 10);
    ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column0, column1, column2},
                                                     IFileWriter::Hints{.row_group_sizes = row_group_sizes}));
    ASSERT_OK(state_->AddDataFiles({data_path}));
  }

  if (drop_table_defer_.has_value()) {
    drop_table_defer_.reset();
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col0", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col1", .type = "text"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "text"}}));
  drop_table_defer_.emplace(std::move(defer));

  ProcessWithFilter("col0, col1, col2", "col1 in ('good1') AND col2 in ('good2')", ExpectedValues());
  auto stats = stats_state_->GetStats(false);

  int result_retrieved = 0;
  int result_skipped = 0;
  for (auto stat : stats) {
    result_retrieved += stat.data().rows_read();
    result_skipped += stat.data().rows_skipped_filter();
  }

  EXPECT_EQ(result_retrieved, match);
  EXPECT_EQ(result_skipped, skipped);
}

}  // namespace
}  // namespace tea
