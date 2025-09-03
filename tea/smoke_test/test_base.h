#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/write.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/mock_teapot.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/stats_state.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/common.h"
#include "tea/test_utils/location.h"
#include "tea/test_utils/metadata.h"

namespace tea {

class ITableCreator {
 public:
  virtual arrow::Result<pq::DropTableDefer> CreateTable(const std::vector<GreenplumColumnInfo>& column_infos,
                                                        const TableName& table_name, const Location& location) = 0;

  virtual ~ITableCreator() = default;
};

class IMetadataWriterBuilder {
 public:
  virtual std::shared_ptr<IMetadataWriter> Build(const TableName& table_name) = 0;

  virtual ~IMetadataWriterBuilder() = default;
};

class TeapotMetadataWriter : public IMetadataWriter {
 public:
  explicit TeapotMetadataWriter(TableName table_name) : table_name_(std::move(table_name)) {}

  arrow::Status AddDataFiles(const std::vector<FilePath>& paths) override {
    for (const auto& path : paths) {
      fragments_.emplace_back(path);
    }
    return arrow::Status::OK();
  }

  arrow::Status AddPositionalDeleteFiles(const std::vector<FilePath>& paths) override {
    for (auto& fragment : fragments_) {
      for (auto& path : paths) {
        fragment = std::move(fragment).AddPositionalDelete(path);
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status AddEqualityDeleteFiles(const std::vector<FilePath>& paths,
                                       const std::vector<int32_t>& field_ids) override {
    for (auto& fragment : fragments_) {
      for (auto& path : paths) {
        fragment = std::move(fragment).AddEqualityDelete(path, field_ids);
      }
    }
    return arrow::Status::OK();
  }

  arrow::Result<Location> Finalize() override {
    auto teapot_ptr = Environment::GetTeapotPtr();
    auto teapot_resp = TeapotExpectedResponse(std::move(fragments_));
    teapot_ptr->SetResponse("db." + table_name_, teapot_resp);
    return Location(TeapotLocation("db", table_name_, teapot_ptr->GetHost(), teapot_ptr->GetPort(),
                                   Options{.profile = Environment::GetProfile()}));
  }

 private:
  const TableName table_name_;
  std::vector<FragmentInfo> fragments_;
};

class TeapotMetadataWriterBuilder : public IMetadataWriterBuilder {
 public:
  std::shared_ptr<IMetadataWriter> Build(const TableName& table_name) override {
    return std::make_shared<TeapotMetadataWriter>(table_name);
  }
};

class IcebergMetadataWriterBuilder : public IMetadataWriterBuilder {
 public:
  std::shared_ptr<IMetadataWriter> Build(const TableName& table_name) override {
    auto maybe_hms_client = Environment::GetHiveMetastoreClient();
    if (!maybe_hms_client.ok()) {
      throw maybe_hms_client.status();
    }
    return std::make_shared<IcebergMetadataWriter>(table_name, maybe_hms_client.ValueUnsafe(),
                                                   Environment::GetProfile());
  }
};

class ExternalTableCreator : public ITableCreator {
 public:
  arrow::Result<pq::DropTableDefer> CreateTable(const std::vector<GreenplumColumnInfo>& column_infos,
                                                const TableName& table_name, const Location& location) override {
    auto& conn = Environment::GetConnWrapper();
    ARROW_ASSIGN_OR_RAISE(auto defer, pq::CreateExternalTableQuery(column_infos, table_name, location).Run(conn));
    return pq::DropTableDefer(std::move(defer));
  }
};

class ForeignTableCreator : public ITableCreator {
 public:
  arrow::Result<pq::DropTableDefer> CreateTable(const std::vector<GreenplumColumnInfo>& column_infos,
                                                const TableName& table_name, const Location& location) override {
    auto& conn = Environment::GetConnWrapper();
    ARROW_ASSIGN_OR_RAISE(auto defer, pq::CreateForeignTableQuery(column_infos, table_name, location).Run(conn));
    return pq::DropTableDefer(std::move(defer));
  }
};

class TestState {
 public:
  TestState(std::shared_ptr<IFileWriter> file_writer, std::shared_ptr<IMetadataWriterBuilder> metadata_writer_builder,
            std::shared_ptr<ITableCreator> table_creator)
      : file_writer_(file_writer), metadata_writer_builder_(metadata_writer_builder), table_creator_(table_creator) {}

  arrow::Result<FilePath> WriteFile(const std::vector<ParquetColumn>& columns, const IFileWriter::Hints& hints = {}) {
    if (columns.empty()) {
      return arrow::Status::ExecutionError("Cannot write file with zero columns");
    }

    return file_writer_->WriteFile(columns, hints);
  }

  arrow::Status AddDataFiles(const std::vector<std::string>& paths, const TableName& table_name = kDefaultTableName) {
    BuildMetadataWriterIfNecessary(table_name);
    return metadata_writer_.at(table_name)->AddDataFiles(paths);
  }

  arrow::Status AddPositionalDeleteFiles(const std::vector<std::string>& paths,
                                         const std::string& table_name = kDefaultTableName) {
    BuildMetadataWriterIfNecessary(table_name);
    return metadata_writer_.at(table_name)->AddPositionalDeleteFiles(paths);
  }

  arrow::Status AddEqualityDeleteFiles(const std::vector<std::string>& paths, const std::vector<int32_t>& field_ids,
                                       const std::string& table_name = kDefaultTableName) {
    BuildMetadataWriterIfNecessary(table_name);
    return metadata_writer_.at(table_name)->AddEqualityDeleteFiles(paths, field_ids);
  }

  arrow::Result<pq::DropTableDefer> CreateTable(const std::vector<GreenplumColumnInfo>& column_infos,
                                                const TableName& table_name = kDefaultTableName) {
    if (!metadata_writer_.contains(table_name)) {
      return arrow::Status::ExecutionError("No metadata for ", table_name,
                                           " is found (check if AddDataFiles was called at least once)");
    }
    ARROW_ASSIGN_OR_RAISE(auto location, metadata_writer_.at(table_name)->Finalize());
    return table_creator_->CreateTable(column_infos, table_name, location);
  }

  void SetFileWriter(std::shared_ptr<IFileWriter> file_writer) { file_writer_ = file_writer; }
  void SetMetadataWriterBuilder(std::shared_ptr<IMetadataWriterBuilder> metadata_writer_builder) {
    metadata_writer_.clear();
    metadata_writer_builder_ = metadata_writer_builder;
  }
  void SetTableCreator(std::shared_ptr<ITableCreator> table_creator) { table_creator_ = table_creator; }

 private:
  std::shared_ptr<IFileWriter> file_writer_;
  std::shared_ptr<IMetadataWriterBuilder> metadata_writer_builder_;
  std::shared_ptr<ITableCreator> table_creator_;

  std::map<TableName, std::shared_ptr<IMetadataWriter>> metadata_writer_;

  void BuildMetadataWriterIfNecessary(const TableName& table_name) {
    if (!metadata_writer_.contains(table_name)) {
      metadata_writer_[table_name] = metadata_writer_builder_->Build(table_name);
    }
  }
};

class TeaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto file_writer = std::make_shared<LocalFileWriter>();

    std::shared_ptr<IMetadataWriterBuilder> metadata_writer_builder;
    if (Environment::GetMetadataType() == MetadataType::kTeapot) {
      metadata_writer_builder = std::make_shared<TeapotMetadataWriterBuilder>();
    } else {
      metadata_writer_builder = std::make_shared<IcebergMetadataWriterBuilder>();
    }

    std::shared_ptr<ITableCreator> table_creator;
    if (Environment::GetTableType() == TestTableType::kExternal) {
      table_creator = std::make_shared<ExternalTableCreator>();
    } else {
      table_creator = std::make_shared<ForeignTableCreator>();
    }

    state_.emplace(file_writer, metadata_writer_builder, table_creator);

    conn_ = &Environment::GetConnWrapper();
    stats_state_ = Environment::GetStatsStatePtr();
    stats_state_->ClearStats();
    stats_state_->ClearFilters();
    teapot_ = Environment::GetTeapotPtr();
    teapot_->ClearResponses();
  }

  pq::PGconnWrapper* conn_;
  StatsState* stats_state_;
  MockTeapot* teapot_;

  std::optional<TestState> state_;
};

}  // namespace tea
