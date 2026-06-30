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

  uint32_t total_files_read = 0;
  for (const auto& stat : stats_state_->GetStats(false)) {
    if (Environment::GetProfile() != "samovar") {
      EXPECT_GE(stat.data().data_files_read(), 1);
    }
    total_files_read += stat.data().data_files_read();
  }

  EXPECT_EQ(total_files_read, 6);
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

TEST_F(OtherEngineGeneratedTable, SnapshotSelectionLatest) {
  std::vector<GreenplumColumnInfo> columns = {GreenplumColumnInfo{.name = "c1", .type = "int4"},
                                              GreenplumColumnInfo{.name = "c2", .type = "int4"}};

  auto ice_loc = IcebergLocation("test", "snapshot_selection", Options{.profile = Environment::GetProfile()});
  auto loc = Location(std::move(ice_loc));
  std::optional<pq::DropTableDefer> defer;
  if (Environment::GetTableType() == TestTableType::kForeign) {
    ASSIGN_OR_FAIL(auto d, pq::CreateForeignTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  } else {
    ASSIGN_OR_FAIL(auto d, pq::CreateExternalTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  }

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).Run(*conn_));
  EXPECT_EQ(result.values.size(), 8);
}

TEST_F(OtherEngineGeneratedTable, SnapshotSelectionBranch) {
  auto create_table = [&](const std::string& gp_table_name, const std::vector<GreenplumColumnInfo>& columns,
                          const std::string& branch) -> arrow::Result<pq::DropTableDefer> {
    auto ice_loc =
        IcebergLocation("mydb", "multiple_branches", Options{.profile = Environment::GetProfile(), .branch = branch});
    auto loc = Location(std::move(ice_loc));
    if (Environment::GetTableType() == TestTableType::kForeign) {
      ARROW_ASSIGN_OR_RAISE(auto result, pq::CreateForeignTableQuery(columns, gp_table_name, loc).Run(*conn_));
      return pq::DropTableDefer(std::move(result));
    }
    ARROW_ASSIGN_OR_RAISE(auto result, pq::CreateExternalTableQuery(columns, gp_table_name, loc).Run(*conn_));
    return pq::DropTableDefer(std::move(result));
  };

  const std::string main_table = kDefaultTableName + "_main";
  const std::string new_branch_table = kDefaultTableName + "_new_branch";

  std::optional<pq::DropTableDefer> main_defer;
  std::optional<pq::DropTableDefer> new_branch_defer;

  std::vector<GreenplumColumnInfo> main_columns = {GreenplumColumnInfo{.name = "a", .type = "int4"}};
  std::vector<GreenplumColumnInfo> new_branch_columns = {GreenplumColumnInfo{.name = "a", .type = "int4"}};

  ASSIGN_OR_FAIL(auto main_drop, create_table(main_table, main_columns, "main"));
  main_defer.emplace(std::move(main_drop));

  ASSIGN_OR_FAIL(auto new_branch_drop, create_table(new_branch_table, new_branch_columns, "new_branch"));
  new_branch_defer.emplace(std::move(new_branch_drop));

  ASSIGN_OR_FAIL(auto main_result, pq::TableScanQuery(main_table).Run(*conn_));
  EXPECT_EQ(main_result, pq::ScanResult({"a"}, {{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}}));

  ASSIGN_OR_FAIL(auto new_branch_result, pq::TableScanQuery(new_branch_table).Run(*conn_));
  EXPECT_EQ(new_branch_result, pq::ScanResult({"a"}, {{"1"}, {"2"}, {"3"}}));
}

// A branch uses the table's current schema (column "a" only), even though the branch's snapshot was written with
// schema (a, b). Declaring the column "b" must therefore fail: it does not exist in the current schema.
// See https://iceberg.apache.org/docs/latest/branching/#schema-selection-with-branches-and-tags
TEST_F(OtherEngineGeneratedTable, SnapshotSelectionBranchMissingColumn) {
  auto ice_loc = IcebergLocation("mydb", "multiple_branches",
                                 Options{.profile = Environment::GetProfile(), .branch = "new_branch"});
  auto loc = Location(std::move(ice_loc));

  std::vector<GreenplumColumnInfo> columns = {GreenplumColumnInfo{.name = "a", .type = "int4"},
                                              GreenplumColumnInfo{.name = "b", .type = "int4"}};

  std::optional<pq::DropTableDefer> defer;
  if (Environment::GetTableType() == TestTableType::kForeign) {
    ASSIGN_OR_FAIL(auto d, pq::CreateForeignTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  } else {
    ASSIGN_OR_FAIL(auto d, pq::CreateExternalTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  }

  auto maybe_result = pq::TableScanQuery(kDefaultTableName).Run(*conn_);
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  std::string message = maybe_result.status().message();
  EXPECT_TRUE(message.find("Greenplum column 'b' not found in Iceberg schema") != std::string::npos) << message;
}

TEST_F(OtherEngineGeneratedTable, SnapshotSelectionSchemaEvolution) {
  auto create_table = [&](const std::string& gp_table_name, const std::vector<GreenplumColumnInfo>& columns,
                          int64_t snapshot_id) -> arrow::Result<pq::DropTableDefer> {
    auto ice_loc = IcebergLocation("mydb", "multiple_branches",
                                   Options{.profile = Environment::GetProfile(), .snapshot_id = snapshot_id});
    auto loc = Location(std::move(ice_loc));
    if (Environment::GetTableType() == TestTableType::kForeign) {
      ARROW_ASSIGN_OR_RAISE(auto result, pq::CreateForeignTableQuery(columns, gp_table_name, loc).Run(*conn_));
      return pq::DropTableDefer(std::move(result));
    }
    ARROW_ASSIGN_OR_RAISE(auto result, pq::CreateExternalTableQuery(columns, gp_table_name, loc).Run(*conn_));
    return pq::DropTableDefer(std::move(result));
  };

  const std::string old_schema_table = kDefaultTableName + "_old_schema";
  const std::string new_schema_table = kDefaultTableName + "_new_schema";

  std::optional<pq::DropTableDefer> old_schema_defer;
  std::optional<pq::DropTableDefer> new_schema_defer;

  std::vector<GreenplumColumnInfo> old_schema_columns = {GreenplumColumnInfo{.name = "a", .type = "int4"},
                                                         GreenplumColumnInfo{.name = "b", .type = "int4"}};
  std::vector<GreenplumColumnInfo> new_schema_columns = {GreenplumColumnInfo{.name = "a", .type = "int4"}};

  ASSIGN_OR_FAIL(auto old_schema_drop, create_table(old_schema_table, old_schema_columns, 7178038598552999887LL));
  old_schema_defer.emplace(std::move(old_schema_drop));

  ASSIGN_OR_FAIL(auto new_schema_drop, create_table(new_schema_table, new_schema_columns, 4741018521999590032LL));
  new_schema_defer.emplace(std::move(new_schema_drop));

  ASSIGN_OR_FAIL(auto old_schema_result, pq::TableScanQuery(old_schema_table).Run(*conn_));
  EXPECT_EQ(old_schema_result, pq::ScanResult({"a", "b"}, {{"1", "1"}, {"2", "1"}, {"3", "2"}}));

  ASSIGN_OR_FAIL(auto new_schema_result, pq::TableScanQuery(new_schema_table).Run(*conn_));
  EXPECT_EQ(new_schema_result, pq::ScanResult({"a"}, {{"1"}, {"2"}, {"3"}, {"4"}, {"5"}, {"6"}}));
}

TEST_F(OtherEngineGeneratedTable, SnapshotSelectionEmpty) {
  std::vector<GreenplumColumnInfo> columns = {GreenplumColumnInfo{.name = "c1", .type = "int4"},
                                              GreenplumColumnInfo{.name = "c2", .type = "int4"}};

  auto ice_loc = IcebergLocation("test", "snapshot_selection",
                                 Options{.profile = Environment::GetProfile(), .snapshot_id = 2425900280988415891LL});
  auto loc = Location(std::move(ice_loc));
  std::optional<pq::DropTableDefer> defer;
  if (Environment::GetTableType() == TestTableType::kForeign) {
    ASSIGN_OR_FAIL(auto d, pq::CreateForeignTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  } else {
    ASSIGN_OR_FAIL(auto d, pq::CreateExternalTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  }

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).Run(*conn_));
  EXPECT_EQ(result.values.size(), 0);
}

TEST_F(OtherEngineGeneratedTable, SnapshotSelectionNonEmpty) {
  std::vector<GreenplumColumnInfo> columns = {GreenplumColumnInfo{.name = "c1", .type = "int4"},
                                              GreenplumColumnInfo{.name = "c2", .type = "int4"}};

  auto ice_loc = IcebergLocation("test", "snapshot_selection",
                                 Options{.profile = Environment::GetProfile(), .snapshot_id = 3335409763063846084LL});
  auto loc = Location(std::move(ice_loc));
  std::optional<pq::DropTableDefer> defer;
  if (Environment::GetTableType() == TestTableType::kForeign) {
    ASSIGN_OR_FAIL(auto d, pq::CreateForeignTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  } else {
    ASSIGN_OR_FAIL(auto d, pq::CreateExternalTableQuery(columns, kDefaultTableName, loc).Run(*conn_));
    defer.emplace(std::move(d));
  }

  ASSIGN_OR_FAIL(auto result, pq::TableScanQuery(kDefaultTableName).Run(*conn_));
  EXPECT_EQ(result.values.size(), 8);
}

TEST_F(OtherEngineGeneratedTable, SnapshotSelectionNonExistent) {
  std::vector<GreenplumColumnInfo> columns = {GreenplumColumnInfo{.name = "c1", .type = "int4"},
                                              GreenplumColumnInfo{.name = "c2", .type = "int4"}};

  auto ice_loc = IcebergLocation("test", "snapshot_selection",
                                 Options{.profile = Environment::GetProfile(), .snapshot_id = 999999LL});
  auto loc = Location(std::move(ice_loc));
  if (Environment::GetTableType() == TestTableType::kForeign) {
    auto res = pq::CreateForeignTableQuery(columns, kDefaultTableName, loc).Run(*conn_);
    if (res.ok()) {
      auto scan_res = pq::TableScanQuery(kDefaultTableName).Run(*conn_);
      EXPECT_FALSE(scan_res.ok());
    }
  } else {
    auto res = pq::CreateExternalTableQuery(columns, kDefaultTableName, loc).Run(*conn_);
    if (res.ok()) {
      auto scan_res = pq::TableScanQuery(kDefaultTableName).Run(*conn_);
      EXPECT_FALSE(scan_res.ok());
    }
  }
}

}  // namespace
}  // namespace tea
