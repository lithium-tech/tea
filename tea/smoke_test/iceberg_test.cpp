#include <arrow/status.h>

#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/spark_generated_test_base.h"
#include "tea/test_utils/common.h"
#include "tea/test_utils/location.h"
#include "tea/test_utils/metadata.h"

namespace tea {
namespace {

// PROJECT_DIR/test/iceberg/gen/gperov_test.py
TEST_F(OtherEngineGeneratedTable, SanityCheck) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).Run(*conn_));

  uint32_t rows_retrieved = result.values.size();
  uint32_t rows_expected = 10'000 - 1;
  EXPECT_EQ(rows_retrieved, rows_expected);

  for (const auto& stat : stats_state_->GetStats(false)) {
    EXPECT_GE(stat.data().data_files_read(), 1);
  }
}

TEST_F(OtherEngineGeneratedTable, EmptyTable) {
  CreateTable("empty", "empty",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).Run(*conn_));

  uint32_t rows_retrieved = result.values.size();
  uint32_t rows_expected = 0;
  EXPECT_EQ(rows_retrieved, rows_expected);

  for (const auto& stat : stats_state_->GetStats(false)) {
    EXPECT_GE(stat.data().data_files_read(), 0);
  }
}

TEST_F(OtherEngineGeneratedTable, IcebergPlanningStats) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).Run(*conn_));

  int32_t bytes_read = 0;
  int32_t files_read = 0;
  int32_t requests = 0;

  for (const auto& stat : stats_state_->GetStats(true)) {
    bytes_read += stat.iceberg().bytes_read();
    files_read += stat.iceberg().files_read();
    requests += stat.iceberg().requests();
  }

  EXPECT_EQ(bytes_read, 21949);
  EXPECT_EQ(files_read, 4);
  EXPECT_EQ(requests, 4);
}

TEST_F(OtherEngineGeneratedTable, QaUpdateTable) {
  CreateTable("qa", "update_null",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "id", .type = "int8"},
                                               GreenplumColumnInfo{.name = "text_field", .type = "text"},
                                               GreenplumColumnInfo{.name = "int_field", .type = "int4"}});
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).Run(*conn_));

  uint32_t rows_retrieved = result.values.size();
  uint32_t rows_expected = 7;
  EXPECT_EQ(rows_retrieved, rows_expected);

  int32_t positional_delete_skipped_rows = 0;
  for (const auto& stat : stats_state_->GetStats(false)) {
    positional_delete_skipped_rows += stat.data().rows_skipped_positional_delete();
  }

  EXPECT_EQ(positional_delete_skipped_rows, 7);
}

TEST_F(OtherEngineGeneratedTable, MinMaxFilters) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).SetWhere("a > 100000").Run(*conn_));

  uint32_t rows_retrieved = result.values.size();
  uint32_t rows_expected = 0;
  EXPECT_EQ(rows_retrieved, rows_expected);

  for (const auto& stat : stats_state_->GetStats(false)) {
    EXPECT_EQ(stat.data().data_files_read(), 0);

    {
      const auto& duration = stat.durations();

      auto filter_build = StatsState::DurationToNanos(duration.filter_build());
      auto filter_apply = StatsState::DurationToNanos(duration.filter_apply());
      auto convert = StatsState::DurationToNanos(duration.convert());
      auto heap_form_tuple = StatsState::DurationToNanos(duration.heap_form_tuple());

      EXPECT_EQ(filter_build, 0);
      EXPECT_EQ(filter_apply, 0);
      EXPECT_EQ(convert, 0);
      EXPECT_EQ(heap_form_tuple, 0);
    }
  }
}

TEST_F(OtherEngineGeneratedTable, MinMaxFilters2) {
  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).SetWhere("a > 5000").Run(*conn_));

  uint32_t rows_retrieved = result.values.size();
  uint32_t rows_expected = 4'999;
  EXPECT_EQ(rows_retrieved, rows_expected);

  for (const auto& stat : stats_state_->GetStats(false)) {
    EXPECT_GE(stat.data().data_files_read(), 1);

    {
      const auto& duration = stat.durations();

      auto filter_build = StatsState::DurationToNanos(duration.filter_build());
      auto filter_apply = StatsState::DurationToNanos(duration.filter_apply());
      auto convert = StatsState::DurationToNanos(duration.convert());
      auto heap_form_tuple = StatsState::DurationToNanos(duration.heap_form_tuple());

      EXPECT_GT(filter_build, 0);
      EXPECT_GT(filter_apply, 0);
      EXPECT_GT(convert, 0);
      EXPECT_GT(heap_form_tuple, 0);
    }
  }
}

TEST_F(TeaTest, OverridingWorks) {
  if (Environment::GetMetadataType() != MetadataType::kIceberg || Environment::GetProfile() != "samovar") {
    GTEST_SKIP();
  }

  for (const std::string& iceberg_table_name : std::vector<std::string>{"simple_table", "overrided_table"}) {
    auto file_writer = std::make_shared<LocalFileWriter>();

    std::shared_ptr<IMetadataWriterBuilder> metadata_writer_builder;
    metadata_writer_builder = std::make_shared<IcebergMetadataWriterBuilder>();

    ASSIGN_OR_FAIL(auto hms_client, Environment::GetHiveMetastoreClient());
    std::shared_ptr<IcebergMetadataWriter> metadata_writer =
        std::make_shared<IcebergMetadataWriter>(iceberg_table_name, hms_client);

    std::shared_ptr<ITableCreator> table_creator;
    if (Environment::GetTableType() == TestTableType::kExternal) {
      table_creator = std::make_shared<ExternalTableCreator>();
    } else {
      table_creator = std::make_shared<ForeignTableCreator>();
    }

    auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8, 9});
    auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
    ASSIGN_OR_FAIL(auto data_path, file_writer->WriteFile({column1, column2}, IFileWriter::Hints{}));
    ASSERT_OK(metadata_writer->AddDataFiles({data_path}));

    ASSIGN_OR_FAIL(auto iceberg_location, metadata_writer->Finalize());

    SimpleLocation location("test-tmp-db", iceberg_table_name);
    ASSIGN_OR_FAIL(auto defer, table_creator->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                           GreenplumColumnInfo{.name = "col2", .type = "int8"}},
                                                          kDefaultTableName, Location(location)));

    if (iceberg_table_name == "overrided_table") {
      ASSIGN_OR_FAIL(pq::ScanResult result, pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_));
      auto expected = pq::ScanResult({"col1"}, {{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}, {"7"}, {"8"}, {"9"}});

      EXPECT_EQ(result, expected);
    } else {
      auto maybe_result = pq::TableScanQuery(kDefaultTableName, "col1").Run(*conn_);
      ASSERT_NE(maybe_result.status(), arrow::Status::OK());

      std::string message = maybe_result.status().message();
      EXPECT_TRUE(message.find("Teapot error") != std::string::npos);
      EXPECT_TRUE(message.find("test-tmp-db.simple_table not found") != std::string::npos);
    }
  }
}

}  // namespace
}  // namespace tea
