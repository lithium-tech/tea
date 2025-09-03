#include <arrow/status.h>

#include <random>
#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/teapot_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/common.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class PositionalDeleteTest : public TeaTest {
 public:
  IFileWriter::Hints FromRgSizes(std::vector<size_t> rg_sizes) {
    return IFileWriter::Hints{.row_group_sizes = std::move(rg_sizes)};
  }
};

TEST_F(PositionalDeleteTest, MissingFile) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  const std::string& file = "file:///to/be/or/not/to/be";
  ASSERT_OK(state_->AddPositionalDeleteFiles({file}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  EXPECT_NE(pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_).status(), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, OneFragment) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({9})));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2, 6, 8});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"1"}, {"2"}, {"4"}, {"5"}, {"6"}, {"8"}});

  EXPECT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);

  int64_t positional_delete_files_read = 0;
  int64_t positional_delete_rows_read = 0;

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    positional_delete_files_read += stat.positional_delete().files_read();
    positional_delete_rows_read += stat.positional_delete().rows_read();

    {
      if (stat.positional_delete().files_read() > 0) {
        const auto& duration = stat.durations();

        auto positional = StatsState::DurationToNanos(duration.positional());
        auto equality = StatsState::DurationToNanos(duration.equality());

        EXPECT_GT(positional, 0);
        EXPECT_EQ(equality, 0);
      }
    }
  });

  EXPECT_EQ(positional_delete_files_read, 1);
  EXPECT_EQ(positional_delete_rows_read, 3);
}

TEST_F(PositionalDeleteTest, MultipleLayers) {
  std::string first_data_path;
  std::string prefix_to_reuse;
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3});
    ASSIGN_OR_FAIL(auto data_path,
                   state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::nullopt,
                                                                            .desired_file_suffix = "suf.parquet"}));
    prefix_to_reuse = data_path.substr(0, data_path.find("suf.parquet"));
    first_data_path = data_path;
    ASSERT_OK(state_->AddDataFiles({data_path}));

    auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path});
    auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2});
    ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));
    ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));
  }

  std::string expected_data_path2 = prefix_to_reuse + "suf2.parquet";
  std::string expected_data_path3 = prefix_to_reuse + "suf3.parquet";

  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{4, 5, 6});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3});
    ASSIGN_OR_FAIL(auto data_path,
                   state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::nullopt,
                                                                            .desired_file_suffix = "suf2.parquet"}));
    ASSERT_OK(state_->AddDataFiles({data_path}));

    // Note that row from expected_data_path3 is not deleted becase this file has greater sequence number
    auto column3 = MakeStringColumn(
        "col3", 1, std::vector<std::string*>{&first_data_path, &expected_data_path2, &expected_data_path3});
    auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 1, 2});
    ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));
    ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));
  }

  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{7, 8, 9});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3});
    ASSIGN_OR_FAIL(auto data_path,
                   state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::nullopt,
                                                                            .desired_file_suffix = "suf3.parquet"}));
    ASSERT_OK(state_->AddDataFiles({data_path}));

    auto column3 = MakeStringColumn(
        "col3", 1, std::vector<std::string*>{&first_data_path, &expected_data_path2, &expected_data_path3});
    auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{1, 2, 0});
    ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));
    ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"4"}, {"8"}, {"9"}});

  EXPECT_EQ(result, expected);
}

TEST_F(PositionalDeleteTest, MultiFragments) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2, 6, 8});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"1"}, {"2"}, {"4"}, {"5"}, {"6"}, {"8"}});

  EXPECT_EQ(result, expected);
}

TEST_F(PositionalDeleteTest, MissingColumn) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  EXPECT_NE(pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_).status(), arrow::Status::OK());
}

TEST_F(PositionalDeleteTest, MultipleFiles) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data0_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));
  ASSERT_OK(state_->AddDataFiles({data0_path}));

  auto column3 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{10, 11, 12, 13, 14, 15, 16, 17, 18});
  auto column4 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(auto data1_path, state_->WriteFile({column3, column4}, FromRgSizes({3, 3, 3})));
  ASSERT_OK(state_->AddDataFiles({data1_path}));

  auto column5 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data0_path, &data0_path, &data0_path});
  auto column6 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2, 6, 8});
  ASSIGN_OR_FAIL(auto del0_path, state_->WriteFile({column5, column6}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del0_path}));

  auto column7 = MakeStringColumn(
      "col3", 1, std::vector<std::string*>{&data1_path, &data1_path, &data1_path, &data1_path, &data1_path});
  auto column8 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 1, 2, 4, 8});
  ASSIGN_OR_FAIL(auto del1_path, state_->WriteFile({column7, column8}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del1_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"1"}, {"2"}, {"4"}, {"5"}, {"6"}, {"8"}, {"13"}, {"15"}, {"16"}, {"17"}});

  EXPECT_EQ(result, expected);
}

// test this with enabled row group skipping
TEST_F(PositionalDeleteTest, DataRowGroupSkipped) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{3, 3, 3, 2, 2, 2, 3, 3, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 4, 6});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col2 >= 3").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"3"}, {"8"}, {"9"}});

  EXPECT_EQ(result, expected);
}

TEST_F(PositionalDeleteTest, CommonDeleteFile) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3});
  ASSIGN_OR_FAIL(auto data0_path, state_->WriteFile({column1}));
  ASSERT_OK(state_->AddDataFiles({data0_path}));

  auto column2 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{4, 5, 6});
  ASSIGN_OR_FAIL(auto data1_path, state_->WriteFile({column2}));
  ASSERT_OK(state_->AddDataFiles({data1_path}));

  auto column3 =
      MakeStringColumn("col3", 1, std::vector<std::string*>{&data0_path, &data0_path, &data1_path, &data1_path});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 2, 1, 2});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"4"}});

  EXPECT_EQ(result, expected);
}

class RowGroupSkippedInDelete : public PositionalDeleteTest {};

TEST_F(RowGroupSkippedInDelete, NothingToSkip) {
  std::string data_path1;
  std::string data_path2;
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{3, 3, 3, 2});
    ASSIGN_OR_FAIL(data_path1, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data_path1}));
  }
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{5, 6, 7, 8, 9});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{2, 2, 3, 3, 3});
    ASSIGN_OR_FAIL(data_path2, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data_path2}));
  }

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path1, &data_path2, &data_path2});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 0, 2});
  ASSIGN_OR_FAIL(auto del_path,
                 state_->WriteFile({column3, column4}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{3}}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"3"}, {"4"}, {"6"}, {"8"}, {"9"}});

  EXPECT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);
  std::vector<int32_t> delete_rows_read;
  for (const auto& stat : stats) {
    delete_rows_read.emplace_back(stat.positional_delete().rows_read());
  }

  if (Environment::GetProfile() == "samovar") {
    std::sort(delete_rows_read.begin(), delete_rows_read.end());
    std::vector<int32_t> expected_deleted_rows_read_1 = {1, 3};
    std::vector<int32_t> expected_deleted_rows_read_2 = {0, 3};
    EXPECT_TRUE(delete_rows_read == expected_deleted_rows_read_1 || delete_rows_read == expected_deleted_rows_read_2);
  } else {
    std::sort(delete_rows_read.begin(), delete_rows_read.end());
    std::vector<int32_t> expected_deleted_rows_read = {1, 3};
    EXPECT_EQ(delete_rows_read, expected_deleted_rows_read);
  }
}

TEST_F(RowGroupSkippedInDelete, SanityCheck) {
  std::string data_path1;
  std::string data_path2;
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{3, 3, 3, 2});
    ASSIGN_OR_FAIL(data_path1, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data_path1}));
  }
  {
    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{5, 6, 7, 8, 9});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{2, 2, 3, 3, 3});
    ASSIGN_OR_FAIL(data_path2, state_->WriteFile({column1, column2}));
    ASSERT_OK(state_->AddDataFiles({data_path2}));
  }

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path1, &data_path2, &data_path2});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 0, 2});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4},
                                                  IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{1, 1, 1}}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"2"}, {"3"}, {"4"}, {"6"}, {"8"}, {"9"}});

  EXPECT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);
  std::vector<int32_t> delete_rows_read;
  std::vector<int32_t> delete_rows_ingored;
  for (const auto& stat : stats) {
    delete_rows_read.emplace_back(stat.positional_delete().rows_read());
    delete_rows_ingored.emplace_back(stat.positional_delete().rows_ignored());
  }

  if (Environment::GetProfile() == "samovar") {
    ASSERT_EQ(delete_rows_read.size(), 2);
    EXPECT_EQ(delete_rows_read[0] + delete_rows_read[1], 3);
  } else {
    std::sort(delete_rows_read.begin(), delete_rows_read.end());
    std::vector<int32_t> expected_deleted_rows_read = {1, 2};
    EXPECT_EQ(delete_rows_read, expected_deleted_rows_read);

    std::sort(delete_rows_ingored.begin(), delete_rows_ingored.end());
    std::vector<int32_t> expected_deleted_rows_ignored = {0, 1};
    EXPECT_EQ(delete_rows_ingored, expected_deleted_rows_ignored);
  }
}

TEST_F(RowGroupSkippedInDelete, IdealRowGroupDistribution) {
  constexpr int32_t kDataFiles = 9;
  constexpr int32_t kRowsInFile = 100;

  std::vector<std::string> data_path;
  int64_t current_value = 0;
  for (size_t i = 0; i < kDataFiles; ++i) {
    OptionalVector<int64_t> values;
    for (int32_t j = 0; j < kRowsInFile; ++j) {
      values.emplace_back(current_value++);
    }

    auto column1 = MakeInt64Column("col1", 1, values);
    ASSIGN_OR_FAIL(auto dp, state_->WriteFile({column1}));
    ASSERT_OK(state_->AddDataFiles({dp}));

    data_path.emplace_back(dp);
  }

  constexpr int32_t kDeleteFiles = 5;
  std::vector<std::vector<std::pair<int32_t, int64_t>>> deleted_data(kDeleteFiles);

  std::mt19937 rnd(2101);
  constexpr int32_t kDeletedFractionInversed = 3;

  int32_t deleted_rows = 0;
  pq::ScanResult expected_result({"col1"}, {});

  for (int32_t i = 0; i < kDataFiles; ++i) {
    for (int32_t j = 0; j < kRowsInFile; ++j) {
      if (rnd() % kDeletedFractionInversed == 0) {
        ++deleted_rows;
        deleted_data[rnd() % kDeleteFiles].emplace_back(i, j);
        continue;
      }

      int32_t value = i * kRowsInFile + j;
      std::vector<std::string> row{std::to_string(value)};
      expected_result.values.emplace_back(std::move(row));
    }
  }

  for (int32_t i = 0; i < kDeleteFiles; ++i) {
    std::vector<std::string*> paths;
    OptionalVector<int64_t> rows;
    std::vector<size_t> row_group_sizes;
    for (const auto& [file_id, row] : deleted_data[i]) {
      paths.emplace_back(&data_path[file_id]);
      rows.emplace_back(row);
      row_group_sizes.emplace_back(1);
    }

    auto column3 = MakeStringColumn("col3", 1, paths);
    auto column4 = MakeInt64Column("col4", 2, rows);
    ASSIGN_OR_FAIL(auto del_path,
                   state_->WriteFile({column3, column4}, IFileWriter::Hints{.row_group_sizes = row_group_sizes}));
    ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));

  EXPECT_EQ(result, expected_result);

  auto stats = stats_state_->GetStats(false);
  int32_t total_read_positional_rows = 0;
  int32_t total_skipped_data_rows = 0;
  int32_t total_ignored_delete_rows = 0;

  for (const auto& stat : stats) {
    total_read_positional_rows += stat.positional_delete().rows_read();
    total_skipped_data_rows += stat.data().rows_skipped_positional_delete();
    total_ignored_delete_rows += stat.positional_delete().rows_ignored();
  }

  EXPECT_EQ(deleted_rows, total_read_positional_rows);
  EXPECT_EQ(deleted_rows, total_skipped_data_rows);
  EXPECT_GT(total_ignored_delete_rows, 0);
}

TEST_F(RowGroupSkippedInDelete, RowGroupsWithMultipleRows) {
  constexpr int32_t kDataFiles = 9;
  constexpr int32_t kRowsInFile = 100;

  std::vector<std::string> data_path;
  int64_t current_value = 0;
  for (size_t i = 0; i < kDataFiles; ++i) {
    OptionalVector<int64_t> values;
    for (int32_t j = 0; j < kRowsInFile; ++j) {
      values.emplace_back(current_value++);
    }

    auto column1 = MakeInt64Column("col1", 1, values);
    ASSIGN_OR_FAIL(auto dp, state_->WriteFile({column1}));
    ASSERT_OK(state_->AddDataFiles({dp}));

    data_path.emplace_back(dp);
  }

  constexpr int32_t kDeleteFiles = 5;
  std::vector<std::vector<std::pair<int32_t, int64_t>>> deleted_data(kDeleteFiles);

  std::mt19937 rnd(2101);
  constexpr int32_t kDeletedFractionInversed = 3;
  constexpr int32_t kRowGroupSize = 3;

  int32_t deleted_rows = 0;
  pq::ScanResult expected_result({"col1"}, {});

  for (int32_t i = 0; i < kDataFiles; ++i) {
    for (int32_t j = 0; j < kRowsInFile; ++j) {
      if (rnd() % kDeletedFractionInversed == 0) {
        ++deleted_rows;
        deleted_data[rnd() % kDeleteFiles].emplace_back(i, j);
        continue;
      }

      int32_t value = i * kRowsInFile + j;
      std::vector<std::string> row{std::to_string(value)};
      expected_result.values.emplace_back(std::move(row));
    }
  }

  for (int32_t i = 0; i < kDeleteFiles; ++i) {
    std::vector<std::string*> paths;
    OptionalVector<int64_t> rows;
    std::vector<size_t> row_group_sizes;
    for (const auto& [file_id, row] : deleted_data[i]) {
      paths.emplace_back(&data_path[file_id]);
      rows.emplace_back(row);
    }

    int32_t t = 0;
    for (; t + kRowGroupSize < static_cast<int32_t>(paths.size()); t += kRowGroupSize) {
      row_group_sizes.emplace_back(kRowGroupSize);
    }

    row_group_sizes.emplace_back(static_cast<int32_t>(paths.size()) - t);

    auto column3 = MakeStringColumn("col3", 1, paths);
    auto column4 = MakeInt64Column("col4", 2, rows);
    ASSIGN_OR_FAIL(auto del_path,
                   state_->WriteFile({column3, column4}, IFileWriter::Hints{.row_group_sizes = row_group_sizes}));
    ASSERT_OK(state_->AddPositionalDeleteFiles({del_path}));
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));

  EXPECT_EQ(result, expected_result);

  auto stats = stats_state_->GetStats(false);
  int32_t total_read_positional_rows = 0;
  int32_t total_skipped_data_rows = 0;

  for (const auto& stat : stats) {
    total_read_positional_rows += stat.positional_delete().rows_read();
    total_skipped_data_rows += stat.data().rows_skipped_positional_delete();
  }

  EXPECT_LE(deleted_rows, total_read_positional_rows);
  EXPECT_LT(total_read_positional_rows, deleted_rows * 1.5);
  EXPECT_EQ(deleted_rows, total_skipped_data_rows);
}

class PositionalDeleteTeapotTest : public TeapotTest {};

TEST_F(PositionalDeleteTeapotTest, HoleInFile) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{3, 3, 3, 2, 2, 2, 3, 3, 3});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1, column2}, FromRgSizes({3, 3, 3})));

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{0, 4, 6});
  ASSIGN_OR_FAIL(auto del_path, state_->WriteFile({column3, column4}));

  auto offsets = GetParquetRowGroupOffsets(data_path);

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  SetTeapotResponse(
      std::vector<FragmentInfo>{FragmentInfo(data_path).AddPositionalDelete(del_path).SetFromTo(offsets[0], offsets[1]),
                                FragmentInfo(data_path).AddPositionalDelete(del_path).SetPosition(offsets[2])});

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col2 >= 3").Run(*conn_));
  pq::ScanResult expected_result({"col1"}, {{"2"}, {"3"}, {"8"}, {"9"}});

  EXPECT_EQ(result, expected_result);
}

}  // namespace
}  // namespace tea
