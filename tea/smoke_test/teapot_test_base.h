#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "tea/smoke_test/test_base.h"

namespace tea {

class TrivialTeapotMetadataWriter : public TeapotMetadataWriter {
 public:
  TrivialTeapotMetadataWriter() : TeapotMetadataWriter(kDefaultTableName) {}

  arrow::Result<Location> Finalize() override {
    auto teapot_ptr = Environment::GetTeapotPtr();
    return Location(TeapotLocation("db", kDefaultTableName, teapot_ptr->GetHost(), teapot_ptr->GetPort(),
                                   Options{.profile = Environment::GetProfile()}));
  }

  void SetTeapotResponse(const std::vector<FragmentInfo>& fragments) {
    auto teapot_ptr = Environment::GetTeapotPtr();
    teapot_ptr->SetResponse("db." + kDefaultTableName, TeapotExpectedResponse(fragments));
  }
};

class TrivialTeapotMetadataWriterBuilder : public IMetadataWriterBuilder {
 public:
  explicit TrivialTeapotMetadataWriterBuilder(std::shared_ptr<TrivialTeapotMetadataWriter> instance)
      : instance_(instance) {}

  std::shared_ptr<IMetadataWriter> Build(const TableName& table_name) override { return instance_; }

 private:
  std::shared_ptr<TrivialTeapotMetadataWriter> instance_;
};

class TeapotTest : public TeaTest {
 public:
  void SetUp() override {
    if (Environment::GetMetadataType() != MetadataType::kTeapot) {
      GTEST_SKIP() << "Skip test only for teapot";
    }
    TeaTest::SetUp();
    metadata_writer_ = std::make_shared<TrivialTeapotMetadataWriter>();

    state_->SetMetadataWriterBuilder(std::make_shared<TrivialTeapotMetadataWriterBuilder>(metadata_writer_));
    ASSERT_OK(state_->AddDataFiles({}));
  }

  IFileWriter::Hints FromRgSizes(std::vector<size_t> rg_sizes) {
    return IFileWriter::Hints{.row_group_sizes = std::move(rg_sizes)};
  }

  void SetTeapotResponse(const std::vector<FragmentInfo>& fragments) { metadata_writer_->SetTeapotResponse(fragments); }

 private:
  std::shared_ptr<TrivialTeapotMetadataWriter> metadata_writer_;
};

}  // namespace tea
