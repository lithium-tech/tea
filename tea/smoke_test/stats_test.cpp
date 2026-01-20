#include <iceberg/manifest_entry.h>
#include <iceberg/table_metadata.h>
#include <iceberg/type.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <random>
#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/teapot_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/common.h"
#include "tea/test_utils/metadata.h"
#include "tea/util/measure.h"

namespace tea {
namespace {

class StatsTest : public TeaTest {};

TEST_F(StatsTest, RowsReadSkipped) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{3, 3, 3}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  auto column3 = MakeStringColumn("col3", 1, std::vector<std::string*>{&data_path, &data_path, &data_path});
  auto column4 = MakeInt64Column("col4", 2, OptionalVector<int64_t>{2, 6, 8});
  ASSIGN_OR_FAIL(auto pos_del_path, state_->WriteFile({column3, column4}));
  ASSERT_OK(state_->AddPositionalDeleteFiles({pos_del_path}));

  auto column5 = MakeInt64Column("col4", 1, OptionalVector<int64_t>{3, 7});
  ASSIGN_OR_FAIL(auto eq_del_path, state_->WriteFile({column5}));
  ASSERT_OK(state_->AddEqualityDeleteFiles({eq_del_path}, {1}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 != 4").Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"0"}, {"1"}, {"5"}});
  EXPECT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);

  int64_t rows_read = 0;
  int64_t row_groups_read = 0;
  int64_t rows_skipped_filter = 0;
  int64_t rows_skipped_positional_delete = 0;
  int64_t rows_skipped_equality_delete = 0;

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    rows_read += stat.data().rows_read();
    row_groups_read += stat.data().row_groups_read();
    rows_skipped_filter += stat.data().rows_skipped_filter();
    rows_skipped_positional_delete += stat.data().rows_skipped_positional_delete();
    rows_skipped_equality_delete += stat.data().rows_skipped_equality_delete();
  });

  EXPECT_EQ(rows_read, 3);
  EXPECT_EQ(row_groups_read, 3);
  EXPECT_EQ(rows_skipped_filter, 1);
  EXPECT_EQ(rows_skipped_positional_delete, 3);
  EXPECT_EQ(rows_skipped_equality_delete, 2);
}

TEST_F(StatsTest, SingleFile) {
  if (!(Environment::GetMetadataType() == MetadataType::kIceberg)) {
    GTEST_SKIP() << "Skipping test for iceberg metadata";
  }
  constexpr int64_t kRowGroups = 12;
  constexpr int64_t kRowsInRowGroup = 10'000;

  OptionalVector<int64_t> data(kRowGroups * kRowsInRowGroup);
  std::iota(data.begin(), data.end(), 0);

  auto column = MakeInt64Column("col1", 1, std::move(data));
  ASSIGN_OR_FAIL(auto data_path,
                 state_->WriteFile({std::move(column)}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>(
                                                                               kRowGroups, kRowsInRowGroup)}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "count(col1)").Run(*conn_));

  auto stats = stats_state_->GetStats(false);

  int64_t rows_read = 0;
  int64_t row_groups_read = 0;
  int64_t rows_skipped_filter = 0;
  int64_t rows_skipped_positional_delete = 0;
  int64_t rows_skipped_equality_delete = 0;
  int64_t max_rows_read = 0;
  int64_t min_rows_read = kRowGroups * kRowsInRowGroup;
  int64_t num_segments = 0;

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    rows_read += stat.data().rows_read();
    row_groups_read += stat.data().row_groups_read();
    rows_skipped_filter += stat.data().rows_skipped_filter();
    rows_skipped_positional_delete += stat.data().rows_skipped_positional_delete();
    rows_skipped_equality_delete += stat.data().rows_skipped_equality_delete();

    max_rows_read = std::max(max_rows_read, stat.data().rows_read());
    min_rows_read = std::min(min_rows_read, stat.data().rows_read());

    ++num_segments;
  });

  double rows_read_skew = (static_cast<double>(max_rows_read) - static_cast<double>(min_rows_read)) /
                          (static_cast<double>(rows_read) / num_segments);
  if (Environment::GetProfile() != "samovar") {
    EXPECT_LE(rows_read_skew, 0.3);  // maybe too little
  }
}

struct ScanEverythingParams {
  int32_t partitions = 0;
  int32_t layers_in_partition = 0;
  int32_t data_files_in_layer = 0;
  int32_t row_groups_in_data_file = 0;
  int32_t rows_in_row_group = 0;
  int32_t positional_delete_files_in_layer = 0;
  int32_t rows_in_positional_delete_file = 0;
  int32_t equality_delete_files_in_layer = 0;
  int32_t rows_in_equality_delete_file = 0;

  double max_allowed_skew = 0.3;
};

class ScanEverythingTest : public TeapotTest {
 public:
  void RunTest(const ScanEverythingParams& params) {
    std::mt19937 rnd(0);

    int64_t current_row = 0;
    std::vector<FragmentInfo> all_fragments;

    std::set<int64_t> answer;
    std::map<std::string, int64_t> file_name_to_first_row_value;

    for (int32_t i = 0; i < params.partitions; ++i) {
      std::vector<FragmentInfo> fragments_in_partition;
      const int64_t min_row_in_partition = current_row;
      for (int32_t j = 0; j < params.layers_in_partition; ++j) {
        for (int32_t k = 0; k < params.data_files_in_layer; ++k) {
          // std::cerr << "data (" << i << ", " << j << ", " << k << ")" << std::endl;
          const auto first_row_num = current_row;
          auto suffix = "data_" + std::to_string(i) + "_" + std::to_string(j) + "_" + std::to_string(k) + ".parquet";
          OptionalVector<int64_t> values;
          values.reserve(params.row_groups_in_data_file * params.rows_in_positional_delete_file);
          for (int32_t l = 0; l < params.row_groups_in_data_file; ++l) {
            for (int32_t m = 0; m < params.rows_in_row_group; ++m) {
              answer.insert(current_row);
              values.push_back(current_row++);
            }
          }
          auto column1 = MakeInt64Column("col1", 1, std::move(values));
          std::vector<size_t> row_group_sizes(params.row_groups_in_data_file, params.rows_in_row_group);
          IFileWriter::Hints hints;
          hints.row_group_sizes = row_group_sizes;
          hints.desired_file_suffix = suffix;
          ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, hints));
          file_name_to_first_row_value[data_path] = first_row_num;
          fragments_in_partition.emplace_back(data_path);
        }

        std::uniform_int_distribution<> random_rownum_gen(
            0, params.row_groups_in_data_file * params.rows_in_row_group - 1);
        std::uniform_int_distribution<> random_file_id_gen(0, fragments_in_partition.size() - 1);

        for (int32_t k = 0; k < params.positional_delete_files_in_layer; ++k) {
          // std::cerr << "posdel (" << i << ", " << j << ", " << k << ")" << std::endl;
          std::vector<std::pair<std::string*, int64_t>> pos_del;
          pos_del.reserve(params.rows_in_positional_delete_file);
          for (int32_t l = 0; l < params.rows_in_positional_delete_file; ++l) {
            int64_t row_id = random_rownum_gen(rnd);
            int64_t file_id = random_file_id_gen(rnd);
            pos_del.emplace_back(&fragments_in_partition[file_id].data_path, row_id);
            answer.erase(file_name_to_first_row_value[fragments_in_partition[file_id].data_path] + row_id);
          }
          std::sort(pos_del.begin(), pos_del.end(), [](const auto& lhs, const auto& rhs) {
            return std::tie(*lhs.first, lhs.second) < std::tie(*rhs.first, rhs.second);
          });
          std::vector<std::string*> file_path_column;
          OptionalVector<int64_t> rownum_column;
          file_path_column.reserve(pos_del.size());
          rownum_column.reserve(pos_del.size());
          for (const auto& [file_path, row_id] : pos_del) {
            file_path_column.push_back(file_path);
            rownum_column.push_back(row_id);
          }
          auto suffix = "pos_del_" + std::to_string(i) + "_" + std::to_string(j) + "_" + std::to_string(k) + ".parquet";

          auto column3 = MakeStringColumn("col3", 1, std::move(file_path_column));
          auto column4 = MakeInt64Column("col4", 2, std::move(rownum_column));

          IFileWriter::Hints hints;
          hints.desired_file_suffix = suffix;
          ASSIGN_OR_FAIL(auto pos_del_path, state_->WriteFile({column3, column4}, hints));

          for (auto& frag : fragments_in_partition) {
            frag = std::move(frag).AddPositionalDelete(pos_del_path);
          }
        }

        std::uniform_int_distribution<> random_id_gen(min_row_in_partition, current_row - 1);
        for (int32_t k = 0; k < params.equality_delete_files_in_layer; ++k) {
          // std::cerr << "eqdel (" << i << ", " << j << ", " << k << ")" << std::endl;
          OptionalVector<int64_t> id_column;
          id_column.reserve(params.rows_in_equality_delete_file);
          for (int32_t l = 0; l < params.rows_in_equality_delete_file; ++l) {
            int64_t id = random_id_gen(rnd);
            answer.erase(id);
            id_column.emplace_back(id);
          }
          auto suffix = "eq_del_" + std::to_string(i) + "_" + std::to_string(j) + "_" + std::to_string(k) + ".parquet";

          auto column = MakeInt64Column("col", 1, std::move(id_column));

          IFileWriter::Hints hints;
          hints.desired_file_suffix = suffix;
          ASSIGN_OR_FAIL(auto eq_del_path, state_->WriteFile({column}, hints));
          for (auto& frag : fragments_in_partition) {
            frag = std::move(frag).AddEqualityDelete(eq_del_path, {1});
          }
        }
      }

      all_fragments.insert(all_fragments.end(), std::make_move_iterator(fragments_in_partition.begin()),
                           std::make_move_iterator(fragments_in_partition.end()));
    }

    ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                    GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

    std::vector<FragmentInfo> by_row_group_splitted_fragments = [&]() {
      std::vector<FragmentInfo> result;
      for (const auto& frag : all_fragments) {
        auto offsets = GetParquetRowGroupOffsets(frag.data_path);
        for (size_t i = 0; i + 1 < offsets.size(); ++i) {
          result.emplace_back(frag.GetCopy().SetFromTo(offsets[i], offsets[i + 1]));
        }
        result.emplace_back(frag.GetCopy().SetPosition(offsets.back()));
      }
      return result;
    }();

    auto generate_strange_offsets = [&](int64_t last_offset, int64_t fragments_count) {
      std::uniform_int_distribution<> offset_gen(1, last_offset);

      while (true) {
        std::vector<int64_t> offsets(fragments_count);
        for (auto& off : offsets) {
          off = offset_gen(rnd);
        }
        offsets[0] = 1;
        std::sort(offsets.begin(), offsets.end());
        bool good = true;
        for (size_t i = 0; i + 1 < offsets.size(); ++i) {
          if (offsets[i] == offsets[i + 1]) {
            good = false;
            break;
          }
        }
        if (good) {
          return offsets;
        }
      }
    };

    std::vector<FragmentInfo> strangely_splitted_fragments = [&]() {
      std::uniform_int_distribution<> fragments_count_gen(2, 10);

      std::vector<FragmentInfo> result;
      for (const auto& frag : all_fragments) {
        auto offsets =
            generate_strange_offsets(GetParquetRowGroupOffsets(frag.data_path).back(), fragments_count_gen(rnd));
        for (size_t i = 0; i + 1 < offsets.size(); ++i) {
          result.emplace_back(frag.GetCopy().SetFromTo(offsets[i], offsets[i + 1]));
        }
        result.emplace_back(frag.GetCopy().SetPosition(offsets.back()));
      }
      return result;
    }();

    for (auto fragments_variant : {all_fragments, by_row_group_splitted_fragments, strangely_splitted_fragments}) {
      std::shuffle(fragments_variant.begin(), fragments_variant.end(), rnd);

      SetTeapotResponse(fragments_variant);

      ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));

      if (result.values.size() != answer.size()) {
        EXPECT_EQ(result.values.size(), answer.size());
      } else {
        std::vector<std::vector<std::string>> expected_values(answer.size());
        auto it = answer.begin();
        for (size_t i = 0; i < answer.size(); ++i) {
          expected_values[i].emplace_back(std::to_string(*it));
          ++it;
        }

        pq::ScanResult expected_result(result.headers, std::move(expected_values));

        if (result != expected_result) {
          EXPECT_TRUE(false) << "Wrong result";
          std::cerr << "result.values.size() = " << result.values.size() << std::endl;
          std::cerr << "expected_result.values.size() = " << expected_result.values.size() << std::endl;
          std::sort(expected_result.values.begin(), expected_result.values.end());
          std::sort(result.values.begin(), result.values.end());
          uint64_t different_values_counter = 0;
          uint64_t min_value = result.values.size();
          uint64_t max_value = 0;
          for (size_t i = 0; i < result.values.size(); ++i) {
            if (result.values[i] != expected_result.values[i]) {
              max_value = i;
              if (min_value == result.values.size()) {
                min_value = i;
              }
              ++different_values_counter;
            }
          }
          std::cerr << "different_values_counter = " << different_values_counter << std::endl;
          std::cerr << "min_value = " << min_value << std::endl;
          std::cerr << "max_value = " << max_value << std::endl;
        }
      }

      int64_t rows_result = result.values.size();

      auto stats = stats_state_->GetStats(false);

      int64_t rows_read = 0;
      int64_t row_groups_read = 0;
      int64_t rows_skipped_filter = 0;
      int64_t rows_skipped_positional_delete = 0;
      int64_t rows_skipped_equality_delete = 0;

      int64_t max_positional_delete_files_read = 0;
      int64_t max_equality_delete_files_read = 0;

      int64_t min_rows_read = std::numeric_limits<int64_t>::max();
      int64_t max_rows_read = std::numeric_limits<int64_t>::min();
      int64_t num_segments = 0;

      std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
        ++num_segments;

        rows_read += stat.data().rows_read();
        row_groups_read += stat.data().row_groups_read();
        rows_skipped_filter += stat.data().rows_skipped_filter();
        rows_skipped_positional_delete += stat.data().rows_skipped_positional_delete();
        rows_skipped_equality_delete += stat.data().rows_skipped_equality_delete();
        max_positional_delete_files_read =
            std::max(max_positional_delete_files_read, stat.positional_delete().files_read());
        max_equality_delete_files_read = std::max(max_equality_delete_files_read, stat.equality_delete().files_read());

        min_rows_read = std::min(min_rows_read, stat.data().rows_read());
        max_rows_read = std::max(max_rows_read, stat.data().rows_read());
      });

      EXPECT_EQ(rows_read, rows_result);

      const int64_t kDataFiles = params.partitions * params.layers_in_partition * params.data_files_in_layer;
      const int64_t kRowGroups = kDataFiles * params.row_groups_in_data_file;
      const int64_t kPositionalDeleteFiles =
          params.partitions * params.layers_in_partition * params.positional_delete_files_in_layer;
      const int64_t kEqualityDeleteFiles =
          params.partitions * params.layers_in_partition * params.equality_delete_files_in_layer;

      const int64_t all_rows = kRowGroups * params.rows_in_row_group;

      EXPECT_EQ(rows_result,
                all_rows - rows_skipped_filter - rows_skipped_equality_delete - rows_skipped_positional_delete);

      EXPECT_EQ(row_groups_read, kRowGroups);

      // each delete file must be read exactly once
      const int64_t kMaxPositionalDeleteFilesRead =
          params.partitions * params.layers_in_partition * params.positional_delete_files_in_layer;
      EXPECT_GE(max_positional_delete_files_read, kPositionalDeleteFiles);
      EXPECT_LE(max_positional_delete_files_read, kMaxPositionalDeleteFilesRead);

      const int64_t kMaxEqualitylDeleteFilesRead =
          params.partitions * params.layers_in_partition * params.equality_delete_files_in_layer;
      EXPECT_GE(max_equality_delete_files_read, kEqualityDeleteFiles);
      EXPECT_LE(max_equality_delete_files_read, kMaxEqualitylDeleteFilesRead);

      double rows_read_skew = (static_cast<double>(max_rows_read) - static_cast<double>(min_rows_read)) /
                              (static_cast<double>(rows_read) / num_segments);
      if (Environment::GetProfile() != "samovar") {
        EXPECT_LE(rows_read_skew, params.max_allowed_skew);  // maybe too little
      }
    }
  }
};

class LargeTest : public ScanEverythingTest {};

TEST_F(LargeTest, OnePartitionNoDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 1;
  params.layers_in_partition = 5;
  params.data_files_in_layer = 5;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10'000;
  params.positional_delete_files_in_layer = 0;
  params.rows_in_positional_delete_file = 0;
  params.equality_delete_files_in_layer = 0;
  params.rows_in_equality_delete_file = 0;

  RunTest(params);
}

TEST_F(LargeTest, MultiplePartitionsWithDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 3;
  params.layers_in_partition = 5;
  params.data_files_in_layer = 7;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10'000;
  params.positional_delete_files_in_layer = 2;
  params.rows_in_positional_delete_file = 100;
  params.equality_delete_files_in_layer = 2;
  params.rows_in_equality_delete_file = 100;

  RunTest(params);
}

class ManySmallFilesTest : public ScanEverythingTest {};

TEST_F(ManySmallFilesTest, OnePartitionNoDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 1;
  params.layers_in_partition = 7;
  params.data_files_in_layer = 7;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10;
  params.positional_delete_files_in_layer = 0;
  params.rows_in_positional_delete_file = 0;
  params.equality_delete_files_in_layer = 0;
  params.rows_in_equality_delete_file = 0;

  RunTest(params);
}

TEST_F(ManySmallFilesTest, OneLayerWithDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 1;
  params.layers_in_partition = 1;
  params.data_files_in_layer = 4;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10;
  params.positional_delete_files_in_layer = 1;
  params.rows_in_positional_delete_file = 1;
  params.equality_delete_files_in_layer = 1;
  params.rows_in_equality_delete_file = 1;

  params.max_allowed_skew = 0.7;

  RunTest(params);
}

TEST_F(ManySmallFilesTest, MultiplePartitionsNoDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 3;
  params.layers_in_partition = 5;
  params.data_files_in_layer = 5;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10;
  params.positional_delete_files_in_layer = 0;
  params.rows_in_positional_delete_file = 0;
  params.equality_delete_files_in_layer = 0;
  params.rows_in_equality_delete_file = 0;

  RunTest(params);
}

TEST_F(ManySmallFilesTest, MultiplePartitionsWithDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 3;
  params.layers_in_partition = 5;
  params.data_files_in_layer = 7;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10;
  params.positional_delete_files_in_layer = 2;
  params.rows_in_positional_delete_file = 5;
  params.equality_delete_files_in_layer = 2;
  params.rows_in_equality_delete_file = 5;

  RunTest(params);
}

TEST_F(StatsTest, AllRowsSkippedOneBatch) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 2, 24, 25, -1231});
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));
  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 = 23").Run(*conn_));

  auto stats = stats_state_->GetStats(false);
  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    EXPECT_EQ(stat.data().row_groups_skipped_filter(), 0);
  });
}

TEST_F(StatsTest, AllRowsSkippedMultipleBatches) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{22, 24, 22, 24, 22, 24, 22, 24, 22, 24});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{2, 2, 2, 2, 2}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));
  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 = 23").Run(*conn_));
  EXPECT_EQ(result.values.size(), 0);

  auto stats = stats_state_->GetStats(false);

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    EXPECT_EQ(stat.data().row_groups_skipped_filter(), 0);
  });
}

TEST_F(StatsTest, SomeBatchesSkipped) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{22, 24, 22, 24, 22, 23, 23, 24, 22, 24});
  auto column2 = MakeInt32Column("col2", 2, OptionalVector<int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{2, 2, 2, 2, 2}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));
  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col2").SetWhere("col1 = 23").Run(*conn_));
  auto expected = pq::ScanResult({"col2"}, {{"5"}, {"6"}});
  EXPECT_EQ(result, expected);

  auto stats = stats_state_->GetStats(false);

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    EXPECT_EQ(stat.data().row_groups_skipped_filter(), 0);
  });
}

TEST_F(StatsTest, Limit) {
  constexpr int kPartitions = 4;
  constexpr int kLayers = 2;
  constexpr int kDataFiles = 3;
  constexpr int kRowGroupsInDataFile = 4;
  constexpr int kRowsInRowGroup = 10'000;

  int current_row = 0;

  std::vector<FragmentInfo> all_fragments;

  for (int32_t i = 0; i < kPartitions; ++i) {
    for (int32_t j = 0; j < kLayers; ++j) {
      for (int32_t k = 0; k < kDataFiles; ++k) {
        OptionalVector<int32_t> values;
        values.reserve(kRowGroupsInDataFile * kRowsInRowGroup);
        for (int32_t l = 0; l < kRowGroupsInDataFile; ++l) {
          for (int32_t m = 0; m < kRowsInRowGroup; ++m) {
            values.push_back(current_row++);
          }
        }
        auto column1 = MakeInt32Column("col1", 1, std::move(values));
        std::vector<size_t> row_group_sizes(kRowGroupsInDataFile, kRowsInRowGroup);
        ASSIGN_OR_FAIL(auto data_path,
                       state_->WriteFile({column1}, IFileWriter::Hints{.row_group_sizes = row_group_sizes}));
        ASSERT_OK(state_->AddDataFiles({data_path}));
      }
    }
  }

  for (int i = 0; i < 3; ++i) {
    ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));
    ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT col1 FROM " + kDefaultTableName + " LIMIT 10").Run(*conn_));
    EXPECT_EQ(result.values.size(), 10);
    auto stats = stats_state_->GetStats(false);

    std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
      // prefetch
      EXPECT_LE(stat.data().data_files_read(), 1 * 2);
      EXPECT_LE(stat.data().row_groups_read(), 1 * 2);
      EXPECT_LE(stat.data().rows_read(), kRowsInRowGroup * 2);
    });
  }
}

TEST_F(StatsTest, Redis) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{3, 3, 3}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 != 4").Run(*conn_));
  int64_t samovar_initial_tasks_count = 0;
  int64_t samovar_splitted_tasks_count = 0;
  int64_t samovar_fetched_tasks_count = 0;
  DurationTicks samovar_total_response_duration_ticks = 0;
  int64_t request_count = 0;
  int64_t errors_count = 0;
  DurationTicks samovar_sync_duration = 0;

  auto stats = stats_state_->GetStats(true);

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    samovar_initial_tasks_count += stat.samovar().samovar_initial_tasks_count();
    samovar_splitted_tasks_count += stat.samovar().samovar_splitted_tasks_count();
    samovar_fetched_tasks_count += stat.samovar().samovar_fetched_tasks_count();
    samovar_total_response_duration_ticks += stat.samovar().samovar_total_response_duration_ticks().nanos();
    request_count += stat.samovar().samovar_requests_count();
    errors_count += stat.samovar().samovar_errors_count();
    samovar_sync_duration += stat.durations().samovar_sync().nanos();
  });

  if (Environment::GetProfile() != "samovar" && Environment::GetProfile() != "samovar_0" &&
      Environment::GetProfile() != "samovar_1" && Environment::GetProfile() != "samovar_0_1") {
    EXPECT_EQ(samovar_initial_tasks_count, 0);
    EXPECT_EQ(samovar_splitted_tasks_count, 0);
    EXPECT_LE(samovar_total_response_duration_ticks, 1e-9);
    EXPECT_EQ(request_count, 0);
    EXPECT_EQ(errors_count, 0);
    return;
  }
  // TeapotMetadataWriter DOES NOT split result by row groups
  // IcebergMetadataWriter DOES split result by row groups
  const int32_t tasks_after_splitting = Environment::GetMetadataType() == MetadataType::kIceberg ? 3 : 1;

  EXPECT_EQ(samovar_initial_tasks_count, 1);
  EXPECT_EQ(samovar_splitted_tasks_count, tasks_after_splitting);
  EXPECT_EQ(samovar_fetched_tasks_count, tasks_after_splitting);
  EXPECT_GT(samovar_total_response_duration_ticks, 0.00001);
  EXPECT_EQ(samovar_sync_duration, 0);  // need_sync_on_init is disabled
  EXPECT_GE(request_count, 1);
  /// Some errors due to incorrect first host in config (to test failover).
  EXPECT_GE(errors_count, 1);
}

TEST_F(StatsTest, IcebergAndHms) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{3, 3, 3}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 != 4").Run(*conn_));
  int64_t iceberg_bytes_read = 0;
  int64_t iceberg_files_read = 0;
  int64_t iceberg_fs_duration_nanos = 0;
  int64_t iceberg_requests = 0;
  int64_t hms_connections = 0;

  auto stats = stats_state_->GetStats(true);

  std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
    iceberg_bytes_read += stat.iceberg().bytes_read();
    iceberg_files_read += stat.iceberg().files_read();
    iceberg_requests += stat.iceberg().requests();
    iceberg_fs_duration_nanos +=
        stat.durations().iceberg_plan_fs().seconds() * 1'000'000'000 + stat.durations().iceberg_plan_fs().nanos();
    hms_connections += stat.hms().connections_established();
  });

  if (Environment::GetMetadataType() != MetadataType::kIceberg) {
    EXPECT_EQ(iceberg_bytes_read, 0);
    EXPECT_EQ(iceberg_files_read, 0);
    EXPECT_EQ(iceberg_requests, 0);
    EXPECT_EQ(iceberg_fs_duration_nanos, 0);
    EXPECT_EQ(hms_connections, 0);
    return;
  }
  EXPECT_GE(iceberg_bytes_read, 1000);
  EXPECT_LE(iceberg_bytes_read, 100000);
  EXPECT_EQ(iceberg_files_read, 3);
  EXPECT_EQ(iceberg_requests, 3);
  EXPECT_GE(iceberg_fs_duration_nanos, 100);
  EXPECT_EQ(hms_connections, 1);
}

class ScanEverythingIcebergTest : public TeaTest {
 public:
  void RunTest(const ScanEverythingParams& params) {
    std::mt19937 rnd(0);

    int64_t current_row = 0;

    std::set<int64_t> answer;
    std::map<std::string, int64_t> file_name_to_first_row_value;

    for (int32_t i = 0; i < params.partitions; ++i) {
      const int64_t min_row_in_partition = current_row;
      std::vector<std::string> data_paths;
      for (int32_t j = 0; j < params.layers_in_partition; ++j) {
        for (int32_t k = 0; k < params.data_files_in_layer; ++k) {
          // std::cerr << "data (" << i << ", " << j << ", " << k << ")" << std::endl;
          const auto first_row_num = current_row;
          auto suffix = "data_" + std::to_string(i) + "_" + std::to_string(j) + "_" + std::to_string(k) + ".parquet";
          OptionalVector<int64_t> values;
          values.reserve(params.row_groups_in_data_file * params.rows_in_positional_delete_file);
          for (int32_t l = 0; l < params.row_groups_in_data_file; ++l) {
            for (int32_t m = 0; m < params.rows_in_row_group; ++m) {
              answer.insert(current_row);
              values.push_back(current_row++);
            }
          }
          auto column1 = MakeInt64Column("col1", 1, std::move(values));
          std::vector<size_t> row_group_sizes(params.row_groups_in_data_file, params.rows_in_row_group);
          IFileWriter::Hints hints;
          hints.row_group_sizes = row_group_sizes;
          hints.desired_file_suffix = suffix;
          ASSIGN_OR_FAIL(auto data_path, state_->WriteFile({column1}, hints));
          file_name_to_first_row_value[data_path] = first_row_num;

          ASSERT_OK(state_->AddDataFiles({data_path}));
          data_paths.emplace_back(data_path);
        }

        std::uniform_int_distribution<> random_rownum_gen(
            0, params.row_groups_in_data_file * params.rows_in_row_group - 1);
        std::uniform_int_distribution<> random_file_id_gen(0, data_paths.size() - 1);

        for (int32_t k = 0; k < params.positional_delete_files_in_layer; ++k) {
          // std::cerr << "posdel (" << i << ", " << j << ", " << k << ")" << std::endl;
          std::vector<std::pair<std::string*, int64_t>> pos_del;
          pos_del.reserve(params.rows_in_positional_delete_file);
          for (int32_t l = 0; l < params.rows_in_positional_delete_file; ++l) {
            int64_t row_id = random_rownum_gen(rnd);
            int64_t file_id = random_file_id_gen(rnd);
            pos_del.emplace_back(&data_paths[file_id], row_id);
            answer.erase(file_name_to_first_row_value[data_paths[file_id]] + row_id);
          }
          std::sort(pos_del.begin(), pos_del.end(), [](const auto& lhs, const auto& rhs) {
            return std::tie(*lhs.first, lhs.second) < std::tie(*rhs.first, rhs.second);
          });
          std::vector<std::string*> file_path_column;
          OptionalVector<int64_t> rownum_column;
          file_path_column.reserve(pos_del.size());
          rownum_column.reserve(pos_del.size());
          for (const auto& [file_path, row_id] : pos_del) {
            file_path_column.push_back(file_path);
            rownum_column.push_back(row_id);
          }
          auto suffix = "pos_del_" + std::to_string(i) + "_" + std::to_string(j) + "_" + std::to_string(k) + ".parquet";

          auto column3 = MakeStringColumn("col3", 1, std::move(file_path_column));
          auto column4 = MakeInt64Column("col4", 2, std::move(rownum_column));

          IFileWriter::Hints hints;
          hints.desired_file_suffix = suffix;
          ASSIGN_OR_FAIL(auto pos_del_path, state_->WriteFile({column3, column4}, hints));

          ASSERT_OK(state_->AddPositionalDeleteFiles({pos_del_path}));
        }

        std::uniform_int_distribution<> random_id_gen(min_row_in_partition, current_row - 1);
        for (int32_t k = 0; k < params.equality_delete_files_in_layer; ++k) {
          // std::cerr << "eqdel (" << i << ", " << j << ", " << k << ")" << std::endl;
          OptionalVector<int64_t> id_column;
          id_column.reserve(params.rows_in_equality_delete_file);
          for (int32_t l = 0; l < params.rows_in_equality_delete_file; ++l) {
            int64_t id = random_id_gen(rnd);
            answer.erase(id);
            id_column.emplace_back(id);
          }
          auto suffix = "eq_del_" + std::to_string(i) + "_" + std::to_string(j) + "_" + std::to_string(k) + ".parquet";

          auto column = MakeInt64Column("col", 1, std::move(id_column));

          IFileWriter::Hints hints;
          hints.desired_file_suffix = suffix;
          ASSIGN_OR_FAIL(auto eq_del_path, state_->WriteFile({column}, hints));
          ASSERT_OK(state_->AddEqualityDeleteFiles({eq_del_path}, {1}));
        }
      }
    }

    ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                    GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

    ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));

    if (result.values.size() != answer.size()) {
      EXPECT_EQ(result.values.size(), answer.size());
    } else {
      std::vector<std::vector<std::string>> expected_values(answer.size());
      auto it = answer.begin();
      for (size_t i = 0; i < answer.size(); ++i) {
        expected_values[i].emplace_back(std::to_string(*it));
        ++it;
      }

      pq::ScanResult expected_result(result.headers, std::move(expected_values));

      if (result != expected_result) {
        EXPECT_TRUE(false) << "Wrong result";
        std::cerr << "result.values.size() = " << result.values.size() << std::endl;
        std::cerr << "expected_result.values.size() = " << expected_result.values.size() << std::endl;
        std::sort(expected_result.values.begin(), expected_result.values.end());
        std::sort(result.values.begin(), result.values.end());
        uint64_t different_values_counter = 0;
        uint64_t min_value = result.values.size();
        uint64_t max_value = 0;
        for (size_t i = 0; i < result.values.size(); ++i) {
          if (result.values[i] != expected_result.values[i]) {
            max_value = i;
            if (min_value == result.values.size()) {
              min_value = i;
            }
            ++different_values_counter;
          }
        }
        std::cerr << "different_values_counter = " << different_values_counter << std::endl;
        std::cerr << "min_value = " << min_value << std::endl;
        std::cerr << "max_value = " << max_value << std::endl;
      }
    }

    int64_t rows_result = result.values.size();

    auto stats = stats_state_->GetStats(false);

    int64_t rows_read = 0;
    int64_t row_groups_read = 0;
    int64_t rows_skipped_filter = 0;
    int64_t rows_skipped_positional_delete = 0;
    int64_t rows_skipped_equality_delete = 0;

    int64_t max_positional_delete_files_read = 0;
    int64_t max_equality_delete_files_read = 0;

    int64_t min_rows_read = std::numeric_limits<int64_t>::max();
    int64_t max_rows_read = std::numeric_limits<int64_t>::min();
    int64_t num_segments = 0;

    std::for_each(stats.begin(), stats.end(), [&](const stats_state::ExecutionStats& stat) {
      ++num_segments;

      rows_read += stat.data().rows_read();
      row_groups_read += stat.data().row_groups_read();
      rows_skipped_filter += stat.data().rows_skipped_filter();
      rows_skipped_positional_delete += stat.data().rows_skipped_positional_delete();
      rows_skipped_equality_delete += stat.data().rows_skipped_equality_delete();
      max_positional_delete_files_read =
          std::max(max_positional_delete_files_read, stat.positional_delete().files_read());
      max_equality_delete_files_read = std::max(max_equality_delete_files_read, stat.equality_delete().files_read());

      min_rows_read = std::min(min_rows_read, stat.data().rows_read());
      max_rows_read = std::max(max_rows_read, stat.data().rows_read());
    });

    EXPECT_EQ(rows_read, rows_result);

    const int64_t kDataFiles = params.partitions * params.layers_in_partition * params.data_files_in_layer;
    const int64_t kRowGroups = kDataFiles * params.row_groups_in_data_file;
    const int64_t kPositionalDeleteFiles =
        params.partitions * params.layers_in_partition * params.positional_delete_files_in_layer;
    const int64_t kEqualityDeleteFiles =
        params.partitions * params.layers_in_partition * params.equality_delete_files_in_layer;

    const int64_t all_rows = kRowGroups * params.rows_in_row_group;

    EXPECT_EQ(rows_result,
              all_rows - rows_skipped_filter - rows_skipped_equality_delete - rows_skipped_positional_delete);

    EXPECT_EQ(row_groups_read, kRowGroups);

    // each delete file must be read exactly once
    const int64_t kMaxPositionalDeleteFilesRead =
        params.partitions * params.layers_in_partition * params.positional_delete_files_in_layer;
    EXPECT_GE(max_positional_delete_files_read, kPositionalDeleteFiles);
    EXPECT_LE(max_positional_delete_files_read, kMaxPositionalDeleteFilesRead);

    const int64_t kMaxEqualitylDeleteFilesRead =
        params.partitions * params.layers_in_partition * params.equality_delete_files_in_layer;
    EXPECT_GE(max_equality_delete_files_read, kEqualityDeleteFiles);
    EXPECT_LE(max_equality_delete_files_read, kMaxEqualitylDeleteFilesRead);

    double rows_read_skew = (static_cast<double>(max_rows_read) - static_cast<double>(min_rows_read)) /
                            (static_cast<double>(rows_read) / num_segments);
    if (Environment::GetProfile() != "samovar") {
      EXPECT_LE(rows_read_skew, params.max_allowed_skew);  // maybe too little
    }
  }
};

class IcebergLargeTest : public ScanEverythingIcebergTest {};

TEST_F(IcebergLargeTest, OnePartitionWithDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 1;
  params.layers_in_partition = 5;
  params.data_files_in_layer = 5;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10'000;
  params.positional_delete_files_in_layer = 10;
  params.rows_in_positional_delete_file = 100;
  params.equality_delete_files_in_layer = 10;
  params.rows_in_equality_delete_file = 100;

  RunTest(params);
}

TEST_F(IcebergLargeTest, MultiplePartitionWithDeletes) {
  if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
    return;
  }

  ScanEverythingParams params;

  params.partitions = 3;
  params.layers_in_partition = 5;
  params.data_files_in_layer = 5;
  params.row_groups_in_data_file = 7;
  params.rows_in_row_group = 10'000;
  params.positional_delete_files_in_layer = 10;
  params.rows_in_positional_delete_file = 100;
  params.equality_delete_files_in_layer = 10;
  params.rows_in_equality_delete_file = 100;

  RunTest(params);
}

class Qwe : public TeaTest {};

TEST_F(Qwe, RowGroupsWithMultipleRows) {
  if (Environment::GetMetadataType() != MetadataType::kIceberg) {
    GTEST_SKIP();
  }
  constexpr int32_t kDataFilesInPartition = 7;
  constexpr int32_t kRowsInFile = 100;

  auto file_writer = std::make_shared<LocalFileWriter>();
  const std::string table_name = "test_table";

  ASSIGN_OR_FAIL(auto hms_client, Environment::GetHiveMetastoreClient());
  iceberg::PartitionField field{.source_id = 2, .field_id = 1001, .name = "p_field", .transform = "identity"};
  std::shared_ptr<IcebergMetadataWriter> metadata_writer = std::make_shared<IcebergMetadataWriter>(
      table_name, hms_client, Environment::GetProfile(), iceberg::PartitionSpec{.spec_id = 0, .fields = {field}});

  std::vector<std::string> data_path;

  std::mt19937 rnd(2101);
  constexpr int32_t kDeletedFractionInversed = 3;
  constexpr int32_t kRowGroupSize = 100;

  int32_t deleted_rows = 0;
  pq::ScanResult expected_result({"col1"}, {});

  int64_t current_value = 0;

  constexpr int32_t kDeleteFilesInPartition = 3;
  constexpr int32_t kPartitions = 7;
  for (int32_t p = 0; p < kPartitions; ++p) {
    iceberg::ContentFile::PartitionKey key("p_field", p,
                                           std::make_shared<iceberg::types::PrimitiveType>(iceberg::TypeID::kInt));
    iceberg::ContentFile::PartitionTuple partition_tuple;
    partition_tuple.fields.emplace_back(key);

    for (size_t i = 0; i < kDataFilesInPartition; ++i) {
      OptionalVector<int64_t> values;
      OptionalVector<int32_t> partiiton_column;
      for (int32_t j = 0; j < kRowsInFile; ++j) {
        values.emplace_back(current_value++);
        partiiton_column.emplace_back(p);
      }

      auto column1 = MakeInt64Column("col1", 1, values);
      auto column2 = MakeInt32Column("p_field", 2, partiiton_column);
      std::string suffix;
      for (int x = 0; x < 6; ++x) {
        suffix += 'a' + (rnd() % 26);
      }
      ASSIGN_OR_FAIL(auto dp,
                     file_writer->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::nullopt,
                                                                                   .desired_file_suffix = suffix}));
      ASSERT_OK(metadata_writer->AddFiles({dp}, iceberg::ContentFile::FileContent::kData, {}, partition_tuple));

      data_path.emplace_back(dp);
    }

    std::vector<std::vector<std::pair<int32_t, int64_t>>> deleted_data(kDeleteFilesInPartition);

    for (int32_t i = 0; i < kDataFilesInPartition; ++i) {
      for (int32_t j = 0; j < kRowsInFile; ++j) {
        if (rnd() % kDeletedFractionInversed == 0) {
          ++deleted_rows;
          deleted_data[rnd() % kDeleteFilesInPartition].emplace_back(i + p * kDataFilesInPartition, j);
          continue;
        }

        int32_t value = p * kDataFilesInPartition * kRowsInFile + i * kRowsInFile + j;
        std::vector<std::string> row{std::to_string(value)};
        expected_result.values.emplace_back(std::move(row));
      }
    }

    for (int32_t i = 0; i < kDeleteFilesInPartition; ++i) {
      std::sort(deleted_data[i].begin(), deleted_data[i].end(), [&](const auto& lhs, const auto& rhs) {
        return std::tie(data_path[lhs.first], lhs.second) < std::tie(data_path[rhs.first], rhs.second);
      });

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
      ASSIGN_OR_FAIL(auto del_path, file_writer->WriteFile({column3, column4},
                                                           IFileWriter::Hints{.row_group_sizes = row_group_sizes}));
      ASSERT_OK(metadata_writer->AddFiles({del_path}, iceberg::ContentFile::FileContent::kPositionDeletes, {},
                                          partition_tuple));
    }
  }

  std::shared_ptr<ITableCreator> table_creator;
  if (Environment::GetTableType() == TestTableType::kExternal) {
    table_creator = std::make_shared<ExternalTableCreator>();
  } else {
    table_creator = std::make_shared<ForeignTableCreator>();
  }

  ASSIGN_OR_FAIL(auto iceberg_location, metadata_writer->Finalize());
  ASSIGN_OR_FAIL(auto defer, table_creator->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"}},
                                                        table_name, iceberg_location));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(table_name, "col1").Run(*conn_));

  EXPECT_EQ(result, expected_result);

  auto stats = stats_state_->GetStats(false);
  int32_t total_read_positional_rows = 0;
  int32_t total_skipped_data_rows = 0;

  for (const auto& stat : stats) {
    total_read_positional_rows += stat.positional_delete().rows_read();
    total_skipped_data_rows += stat.data().rows_skipped_positional_delete();

    EXPECT_LE(stat.positional_delete().files_read(), kPartitions * kDeleteFilesInPartition)
        << "There is delete file read multiple times";
  }

  std::cerr << "total_read_positional_rows = " << total_read_positional_rows << std::endl;
  std::cerr << "deleted_rows = " << deleted_rows << std::endl;

  EXPECT_LE(deleted_rows, total_read_positional_rows);
  EXPECT_LT(total_read_positional_rows, deleted_rows * 2);
  EXPECT_EQ(deleted_rows, total_skipped_data_rows);
}

}  // namespace
}  // namespace tea
