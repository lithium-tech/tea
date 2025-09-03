#include <algorithm>
#include <iterator>
#include <limits>
#include <numeric>
#include <random>
#include <string>

#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "gen/src/generators.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"
#include "parquet/arrow/reader.h"

#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/perf_test/json_table.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class JsonTest : public TeaTest {};

TEST_F(JsonTest, RowsReadSkipped) {
  ScopedTempDir data_dir_;

  gen::RandomDevice random_device(0);

  std::vector<std::pair<int, int>> configurations = {{100, 0}, {99, 1}, {50, 50}, {0, 100}};

  int table_num = 0;
  for (const auto& [non_escaped_patterns, escaped_patterns] : configurations) {
    auto program = gen::MakeJsonProgram(random_device, non_escaped_patterns, escaped_patterns);

    constexpr int64_t kBatchSize = 8192;
    constexpr int64_t kRows = 1'000'000;

    gen::BatchSizeMaker batch_size_maker(kBatchSize, kRows);

    std::string data_path = "file://" + data_dir_.path().string() + "/data" + std::to_string(table_num) + ".parquet";
    const std::string json_table_name = "json_table" + std::to_string(table_num);
    const std::string text_table_name = "text_table" + std::to_string(table_num);
    ++table_num;

    std::string local_file_path = data_path.substr(std::string("file://").size());
    if (data_path.starts_with("file://")) {
      local_file_path = data_path.substr(std::string("file://").size());
    }

    ASSERT_OK(state_->AddDataFiles({data_path}, json_table_name));
    ASSERT_OK(state_->AddDataFiles({data_path}, text_table_name));
    std::cerr << "file_path: " << local_file_path << std::endl;

    auto table = std::make_shared<gen::JsonTable>();
    auto writer = std::make_shared<gen::WriterProcessor>(
        std::make_shared<gen::ParquetWriter>(gen::OpenLocalOutputStream(local_file_path), table->MakeParquetSchema()));

    for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(); rows_in_next_batch != 0;
         rows_in_next_batch = batch_size_maker.NextBatchSize()) {
      ASSIGN_OR_FAIL(auto batch, program.Generate(rows_in_next_batch));
      auto column_names = table->MakeColumnNames();
      ASSIGN_OR_FAIL(auto proj, batch->GetProjection(column_names));
      ASSIGN_OR_FAIL(auto arrow_batch, batch->GetArrowBatch(column_names));
      ASSERT_OK(writer->Process(proj));
    }

    writer.reset();

    ASSIGN_OR_FAIL(
        auto defer1,
        state_->CreateTable({GreenplumColumnInfo{.name = std::string(gen::JsonTable::kJsonColumn), .type = "json"}},
                            json_table_name));

    ASSIGN_OR_FAIL(
        auto defer2,
        state_->CreateTable({GreenplumColumnInfo{.name = std::string(gen::JsonTable::kJsonColumn), .type = "text"}},
                            text_table_name));

    std::vector<std::string> projections{"count(c_json)"};

    for (const auto& projection : projections) {
      for (const std::string& table_name : std::vector<std::string>{json_table_name, text_table_name}) {
        std::cerr << "Selecting " << projection << " from table '" << table_name
                  << "', escaped patterns = " << escaped_patterns << ", nonescaped patterns = " << non_escaped_patterns
                  << std::endl;
        { ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(table_name, projection).Run(*conn_)); }
        { ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(table_name, projection).Run(*conn_)); }
        { auto stats = stats_state_->GetStats(false); }

        ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(table_name, projection).Run(*conn_));

        auto stats = stats_state_->GetStats(false);
        for (const auto& stat : stats) {
          auto duration_to_string = [](const ::google::protobuf::Duration& duration) {
            std::string milli = std::to_string(duration.nanos() / 1'000'000);
            while (milli.size() < 3) {
              milli = std::string("0") + milli;
            }
            return std::to_string(duration.seconds()) + "." + milli;
          };
          std::cerr << "total duration: " << duration_to_string(stat.durations().total()) << std::endl;
          std::cerr << "convert duration: " << duration_to_string(stat.durations().convert()) << std::endl;
        }
        std::cerr << std::string(80, '-') << std::endl;
      }
    }
  }
}

}  // namespace
}  // namespace tea
