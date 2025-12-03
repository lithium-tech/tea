#include <iceberg/manifest_entry.h>

#include <string>

#include "arrow/status.h"
#include "arrow/util/decimal.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class DeletedFilesTest : public TeaTest {};

TEST_F(DeletedFilesTest, Simple) {
  if (Environment::GetMetadataType() != MetadataType::kIceberg) {
    GTEST_SKIP();
  }

  auto file_writer = std::make_shared<LocalFileWriter>();
  const std::string table_name = "test_table";

  ASSIGN_OR_FAIL(auto hms_client, Environment::GetHiveMetastoreClient());
  std::shared_ptr<IcebergMetadataWriter> metadata_writer =
      std::make_shared<IcebergMetadataWriter>(table_name, hms_client, Environment::GetProfile());

  {
    auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 2});
    ASSIGN_OR_FAIL(auto file_path, file_writer->WriteFile({column}, IFileWriter::Hints{}));
    ASSERT_OK(metadata_writer->AddDataFiles({file_path}));
  }

  {
    auto column2 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -50, 50});
    ASSIGN_OR_FAIL(auto file_path2, state_->WriteFile({column2}));
    ASSERT_OK(metadata_writer->AddFiles({file_path2}, iceberg::ContentFile::FileContent::kData, {}, {},
                                        iceberg::ManifestEntry::Status::kDeleted));
  }
  std::shared_ptr<ITableCreator> table_creator;
  if (Environment::GetTableType() == TestTableType::kExternal) {
    table_creator = std::make_shared<ExternalTableCreator>();
  } else {
    table_creator = std::make_shared<ForeignTableCreator>();
  }

  ASSIGN_OR_FAIL(auto iceberg_location, metadata_writer->Finalize());
  ASSIGN_OR_FAIL(auto defer, table_creator->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}},
                                                        table_name, iceberg_location));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"-2"}, {"2"}});
  EXPECT_EQ(result, expected);
}

}  // namespace
}  // namespace tea
