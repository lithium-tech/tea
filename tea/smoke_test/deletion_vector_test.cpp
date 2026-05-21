#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"

namespace tea {
namespace {

arrow::Status CopyDirectoryToS3(const std::filesystem::path& local_dir,
                                const std::shared_ptr<arrow::fs::S3FileSystem>& s3_fs,
                                const std::string& s3_base_path) {
  for (const auto& entry : std::filesystem::recursive_directory_iterator(local_dir)) {
    if (entry.is_regular_file()) {
      auto relative = std::filesystem::relative(entry.path(), local_dir);
      std::string s3_path = s3_base_path + "/" + relative.string();

      auto parent_dir = std::filesystem::path(s3_path).parent_path().string();
      ARROW_RETURN_NOT_OK(s3_fs->CreateDir(parent_dir));

      std::ifstream local_file(entry.path(), std::ios::binary);
      if (!local_file) {
        return arrow::Status::IOError("Failed to open local file: ", entry.path().string());
      }
      std::string content((std::istreambuf_iterator<char>(local_file)), std::istreambuf_iterator<char>());

      ARROW_ASSIGN_OR_RAISE(auto os, s3_fs->OpenOutputStream(s3_path));
      ARROW_RETURN_NOT_OK(os->Write(content));
      ARROW_RETURN_NOT_OK(os->Close());
    }
  }
  return arrow::Status::OK();
}

class DeletionVectorTest : public TeaTest {};

TEST_F(DeletionVectorTest, SimpleScanAndMetrics) {
  if (Environment::GetMetadataType() != MetadataType::kIceberg) {
    GTEST_SKIP() << "Skipping Deletion Vector test for non-Iceberg metadata";
  }

  const std::string table_name = "deletion_vectors";

  std::filesystem::path local_path = "/workspaces/tea/build/_deps/iceberg-cxx-src/tests/tables/deletion_vectors";
  ASSERT_TRUE(std::filesystem::exists(local_path)) << "Could not find deletion_vectors source directory!";

  auto s3_fs = Environment::GetS3Filesystem();
  ASSERT_NE(s3_fs, nullptr);
  std::string s3_base_path = "warehouse/deletion_vectors";
  ASSERT_OK(CopyDirectoryToS3(local_path, s3_fs, s3_base_path));

  ASSIGN_OR_FAIL(auto hms_client, Environment::GetHiveMetastoreClient());
  std::string metadata_location =
      "s3a://" + s3_base_path + "/metadata/00004-ae0294d0-1de5-4fab-97f1-cd80e0078fa4.metadata.json";

  hms_client->CreateTable("test-tmp-db", table_name, metadata_location);
  UploadNessieCatalog("test-tmp-db", table_name, metadata_location);

  std::shared_ptr<ITableCreator> table_creator;
  if (Environment::GetTableType() == TestTableType::kExternal) {
    table_creator = std::make_shared<ExternalTableCreator>();
  } else {
    table_creator = std::make_shared<ForeignTableCreator>();
  }

  auto location = Location(IcebergLocation("test-tmp-db", table_name, Options{.profile = Environment::GetProfile()}));

  ASSIGN_OR_FAIL(auto defer, table_creator->CreateTable({GreenplumColumnInfo{.name = "c1", .type = "int4"},
                                                         GreenplumColumnInfo{.name = "c2", .type = "int4"}},
                                                        table_name, location));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT c1, c2 FROM " + table_name + " ORDER BY c1").Run(*conn_));
  EXPECT_EQ(result.values.size(), 8);

  int64_t rows_skipped_deletion_vector = 0;
  int64_t deletion_vectors_planned = 0;
  int64_t dangling_deletion_vector_files = 0;

  for (const auto& stat : stats_state_->GetStats(true)) {
    rows_skipped_deletion_vector += stat.data().rows_skipped_deletion_vector();
    deletion_vectors_planned += stat.plan().deletion_vectors_planned();
    dangling_deletion_vector_files += stat.plan().dangling_deletion_vector_files();
  }

  EXPECT_EQ(rows_skipped_deletion_vector, 8);
  EXPECT_GE(deletion_vectors_planned, 2);
  EXPECT_EQ(dangling_deletion_vector_files, 0);

  hms_client->DropTable("test-tmp-db", table_name);
}

}  // namespace
}  // namespace tea
