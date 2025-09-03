#include <random>
#include <string>

#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/teapot_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class EqualityDeleteTest : public TeaTest {
 public:
  IFileWriter::Hints FromRgSizes(std::vector<size_t> rg_sizes) {
    return IFileWriter::Hints{.row_group_sizes = std::move(rg_sizes)};
  }
};

TEST_F(EqualityDeleteTest, NonRetrievedAttrInDelete) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeInt64Column("values", 2, OptionalVector<int64_t>{1, 2});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"3"}, {"6"}, {"9"}});
  EXPECT_EQ(result, expected);
}

TEST_F(EqualityDeleteTest, MissingColumn) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeInt64Column("values", 2, OptionalVector<int64_t>{1, 2});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {1, 2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  EXPECT_NE(pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_).status(), arrow::Status::OK());
}

TEST_F(EqualityDeleteTest, EmptyDelete) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeInt64Column("values", 2, OptionalVector<int64_t>{});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}});
  EXPECT_EQ(result, expected);
}

TEST_F(EqualityDeleteTest, MultipleLayers) {
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3});
    ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data_path}));

    auto column3 = MakeInt64Column("values", 2, OptionalVector<int64_t>{1});
    ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
    ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));
  }
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{4, 5, 6});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3});
    ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data_path}));

    auto column3 = MakeInt64Column("values", 2, OptionalVector<int64_t>{2});
    ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
    ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));
  }
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{7, 8, 9});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3});
    ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data_path}));

    auto column3 = MakeInt64Column("values", 2, OptionalVector<int64_t>{3});
    ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
    ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"4"}, {"7"}, {"8"}});
  EXPECT_EQ(result, expected);
}

TEST_F(EqualityDeleteTest, SameDeleteTwice) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeInt64Column("values", 2, OptionalVector<int64_t>{1, 2});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"3"}, {"6"}, {"9"}});
  EXPECT_EQ(result, expected);
}

TEST_F(EqualityDeleteTest, NullInDelete) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{std::nullopt, 2, 3, std::nullopt, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeInt64Column("col3", 2, OptionalVector<int64_t>{std::nullopt});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"3"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}});
  EXPECT_EQ(result, expected);
}

TEST_F(EqualityDeleteTest, MultipleColumnsInDelete) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 =
      MakeInt64Column("col2", 2, OptionalVector<int64_t>{std::nullopt, 1, 2, std::nullopt, 1, 2, std::nullopt, 1, 2});
  auto column3 =
      MakeInt32Column("col3", 3, OptionalVector<int32_t>{std::nullopt, std::nullopt, std::nullopt, 1, 1, 1, 2, 2, 2});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column4 = MakeInt64Column("values1", 2, OptionalVector<int64_t>{std::nullopt, std::nullopt, 2, 1});
  auto column5 = MakeInt32Column("values2", 3, OptionalVector<int32_t>{std::nullopt, 2, std::nullopt, 1});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column4, column5}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del_path}, {2, 3}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"4"}, {"6"}, {"8"}, {"9"}});
  EXPECT_EQ(result, expected);
}

TEST_F(EqualityDeleteTest, OneFragmentMultipleDeletesSameField) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeInt64Column("col3", 2, OptionalVector<int64_t>{1});
  ASSIGN_OR_FAIL(auto del0_path, state_->WriteFile({column3}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del0_path}, {2}));

  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2});
  ASSIGN_OR_FAIL(auto del1_path, state_->WriteFile({column4}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del1_path}, {2}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"3"}, {"6"}, {"9"}});
  EXPECT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);

  int64_t equality_delete_files_read = 0;
  int64_t equality_delete_rows_read = 0;
  int64_t equality_delete_max_row_materialized = 0;

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    equality_delete_files_read += stat.equality_delete().files_read();
    equality_delete_rows_read += stat.equality_delete().rows_read();
    equality_delete_max_row_materialized =
        std::max(equality_delete_max_row_materialized, stat.equality_delete().max_rows_materialized());

    {
      if (stat.equality_delete().files_read() > 0) {
        const auto& duration = stat.durations();

        auto positional = StatsState::DurationToNanos(duration.positional());
        auto equality = StatsState::DurationToNanos(duration.equality());

        EXPECT_EQ(positional, 0);
        EXPECT_GT(equality, 0);
      }
    }
  });

  EXPECT_EQ(equality_delete_files_read, 2);
  EXPECT_EQ(equality_delete_rows_read, 2);
  EXPECT_EQ(equality_delete_max_row_materialized, 2);
}

TEST_F(EqualityDeleteTest, OneFragmentMultipleDeletesDifferentFields) {
  std::string str_a = "a";
  std::string str_b = "b";
  std::string str_c = "c";
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  auto column3 = MakeStringColumn(
      "col3", 42, std::vector<std::string*>{&str_a, &str_a, &str_a, &str_b, &str_b, &str_b, &str_c, &str_c, &str_c});

  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column4 = MakeInt64Column("col3", 2, OptionalVector<int64_t>{1});
  ASSIGN_OR_FAIL(auto del0_path, state_->WriteFile({column4}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del0_path}, {2}));

  auto column5 = MakeStringColumn("col4", 42, std::vector<std::string*>{&str_a});
  ASSIGN_OR_FAIL(auto del1_path, state_->WriteFile({column5}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({del1_path}, {42}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col3", .type = "text"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"5"}, {"6"}, {"8"}, {"9"}});
  EXPECT_EQ(result, expected);
}

class EqualityDeleteTeapotTest : public TeapotTest {};

TEST_F(EqualityDeleteTeapotTest, DifferentDeletesSameData) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));

  auto column3 = MakeInt64Column("col3", 2, OptionalVector<int64_t>{1});
  ASSIGN_OR_FAIL(auto del1_path, state_->WriteFile({column3}));

  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2});
  ASSIGN_OR_FAIL(auto del2_path, state_->WriteFile({column4}));

  auto column5 = MakeInt64Column("col5", 2, OptionalVector<int64_t>{3});
  ASSIGN_OR_FAIL(auto del3_path, state_->WriteFile({column5}));

  auto offsets = GetParquetRowGroupOffsets(data_path);
  SetTeapotResponse({FragmentInfo(data_path).AddEqualityDelete(del1_path, {2}).SetPosition(offsets[0]),
                     FragmentInfo(data_path).AddEqualityDelete(del2_path, {2}).SetPosition(offsets[1]),
                     FragmentInfo(data_path).AddEqualityDelete(del3_path, {2}).SetPosition(offsets[2])});

  EXPECT_NE(pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_).status(), arrow::Status::OK());
}

TEST_F(EqualityDeleteTeapotTest, MultipleDataSameDeletes) {
  constexpr int kDataFiles = 100;
  constexpr int kDeleteFiles = 5;

  std::string str_a = "a";
  std::string str_b = "b";
  std::string str_c = "c";
  std::vector<FilePath> data_paths;
  for (int i = 0; i < kDataFiles; ++i) {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
    auto column3 = MakeStringColumn(
        "col3", 42, std::vector<std::string*>{&str_a, &str_a, &str_a, &str_b, &str_b, &str_b, &str_c, &str_c, &str_c});
    ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
    data_paths.emplace_back(data_path);
  }
  std::vector<std::string> delete_paths;
  for (int i = 0; i < kDeleteFiles; ++i) {
    auto column4 = MakeInt64Column("col3", 2, OptionalVector<int64_t>{2});
    ASSIGN_OR_FAIL(auto delete_path, state_->WriteFile({column4}));
    ASSERT_OK(WriteToFile({column4}, delete_path));
    delete_paths.emplace_back(delete_path);
  }

  std::vector<FragmentInfo> infos;
  for (size_t i = 0; i < kDataFiles; ++i) {
    const std::string& data_path = data_paths[i];
    auto offsets = GetParquetRowGroupOffsets(data_path);
    auto frag = FragmentInfo(data_path);
    for (size_t j = 0; j < i % kDeleteFiles; ++j) {
      frag = std::move(frag).AddEqualityDelete(delete_paths[i % kDeleteFiles], {2});
    }
    infos.emplace_back(std::move(frag));
  }
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}))

  SetTeapotResponse(std::move(infos));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
}

TEST_F(EqualityDeleteTeapotTest, RandomTests) {
  for (const uint32_t kDataFiles : {5, 10, 50}) {
    for (const uint32_t kDeleteFiles : {5, 10, 50}) {
      std::mt19937 rnd(42);

      std::string str_a = "a";
      std::string str_b = "b";
      std::string str_c = "c";
      std::vector<std::string> data_paths;
      for (uint32_t i = 0; i < kDataFiles; ++i) {
        auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
        auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
        auto column3 = MakeStringColumn(
            "col3", 42,
            std::vector<std::string*>{&str_a, &str_a, &str_a, &str_b, &str_b, &str_b, &str_c, &str_c, &str_c});

        ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2, column3}));
        data_paths.emplace_back(data_path);
      }

      std::vector<std::string> delete_paths;
      for (uint32_t i = 0; i < kDeleteFiles; ++i) {
        auto column4 = MakeInt64Column("col3", 2, OptionalVector<int64_t>{2});
        ASSIGN_OR_FAIL(auto delete_path, state_->WriteFile({column4}));
        delete_paths.emplace_back(delete_path);
      }

      ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                      GreenplumColumnInfo{.name = "col2", .type = "int8"},
                                                      GreenplumColumnInfo{.name = "col3", .type = "text"}}));

      std::vector<FragmentInfo> infos;
      for (size_t i = 0; i < kDataFiles; ++i) {
        const std::string& data_path = data_paths[i];
        auto offsets = GetParquetRowGroupOffsets(data_path);
        auto frag = FragmentInfo(data_path);
        uint32_t deletes_to_append = rnd() % kDeleteFiles;
        for (size_t j = 0; j < deletes_to_append; ++j) {
          frag = std::move(frag).AddEqualityDelete(delete_paths[rnd() % kDeleteFiles], {2});
        }
        infos.emplace_back(std::move(frag));
      }
      SetTeapotResponse(std::move(infos));

      ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
    }
  }
}

}  // namespace
}  // namespace tea
