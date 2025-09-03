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
#include "tea/smoke_test/perf_test/table.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

class PerfTest : public TeaTest {};

TEST_F(PerfTest, RowsReadSkipped) {
  ScopedTempDir data_dir_;

  gen::RandomDevice random_device(0);

  auto program = MakeAllTypesProgram(random_device);
  constexpr int64_t kBatchSize = 8192;
  constexpr int64_t kRows = 10'000'000;

  gen::BatchSizeMaker batch_size_maker(kBatchSize, kRows);

  std::string data_path = "file://" + data_dir_.path().string() + "/data.parquet";
  std::string local_file_path = data_path.substr(std::string("file://").size());
  if (data_path.starts_with("file://")) {
    local_file_path = data_path.substr(std::string("file://").size());
  }

  ASSERT_OK(state_->AddDataFiles({data_path}));
  std::cerr << local_file_path << std::endl;

  auto table = std::make_shared<gen::AllTypesTable>();
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

  ASSIGN_OR_FAIL(auto defer,
                 state_->CreateTable(
                     {GreenplumColumnInfo{.name = std::string(gen::AllTypesTable::kInt2Column), .type = "int2"},
                      GreenplumColumnInfo{.name = std::string(gen::AllTypesTable::kInt4Column), .type = "int4"},
                      GreenplumColumnInfo{.name = std::string(gen::AllTypesTable::kInt8Column), .type = "int8"},
                      GreenplumColumnInfo{.name = std::string(gen::AllTypesTable::kStringColumn), .type = "text"}}));

  std::vector<std::string> projections{
      "count(*)", std::string(gen::AllTypesTable::kInt2Column), std::string(gen::AllTypesTable::kInt4Column),
      std::string(gen::AllTypesTable::kInt8Column), std::string(gen::AllTypesTable::kStringColumn)};

  for (const auto& projection : projections) {
    std::cerr << "Selecting " << projection << std::endl;
    ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, projection).Run(*conn_));

    auto stats = stats_state_->GetStats(false);
    for (const auto& stat : stats) {
      stat.PrintDebugString();
    }
    std::cerr << std::string(80, '-') << std::endl;
  }
}

}  // namespace
}  // namespace tea
