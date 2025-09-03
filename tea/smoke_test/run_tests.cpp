#include <arrow/filesystem/s3fs.h>
#include <gtest/gtest.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"

ABSL_FLAG(std::string, metadata_type, "", "Table type to test (teapot or iceberg)");
ABSL_FLAG(std::string, table_type, "", "Table type to test (external or foreign)");
ABSL_FLAG(std::string, profile, "", "Profiles to test");
ABSL_FLAG(bool, verbose, false, "Print logs to stderr");

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  if (FLAGS_table_type.IsSpecifiedOnCommandLine()) {
    auto type = tea::TableTypeFromString(FLAGS_table_type.CurrentValue());
    if (type == std::nullopt) {
      std::cerr << "Unknown table_type" << std::endl;
      return 1;
    }
    tea::Environment::SetTableType(type.value());
  }

  if (FLAGS_profile.IsSpecifiedOnCommandLine()) {
    tea::Environment::SetProfile(FLAGS_profile.CurrentValue());
  }

  if (FLAGS_metadata_type.IsSpecifiedOnCommandLine()) {
    auto type = tea::MetadataTypeFromString(FLAGS_metadata_type.CurrentValue());
    if (type == std::nullopt) {
      std::cerr << "Unknown metadata_type" << std::endl;
      return 1;
    }
    tea::Environment::SetMetadataType(type.value());
  }

  const bool verbose = absl::GetFlag(FLAGS_verbose);

  ::testing::AddGlobalTestEnvironment(new tea::Environment);
  if (verbose) {
    auto status = tea::pq::Command("SET client_min_messages = 'LOG'").Run(tea::Environment::GetConnWrapper());
    if (!status.ok()) {
      std::cerr << status << std::endl;
    }
  }

  auto maybe_client = tea::Environment::GetHiveMetastoreClient();
  if (maybe_client.ok()) {
    auto client = maybe_client.MoveValueUnsafe();
    auto all_databases = client->GetDatabases();
    if (std::find(all_databases.begin(), all_databases.end(), "test-tmp-db") != all_databases.end()) {
      auto tables = client->GetTables("test-tmp-db");
      for (const auto& table : tables) {
        client->DropTable("test-tmp-db", table);
      }
      client->DropDatabase("test-tmp-db");
    }
    client->CreateDatabase("test-tmp-db", "");
  }

  auto res = RUN_ALL_TESTS();
  if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
    arrow::fs::FinalizeS3().ok();
  }
  return res;
}
