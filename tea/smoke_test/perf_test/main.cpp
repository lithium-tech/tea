#include <arrow/filesystem/s3fs.h>
#include <gtest/gtest.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "tea/smoke_test/environment.h"

ABSL_FLAG(std::string, table_type, "", "Table type to test (external or foreign)");
ABSL_FLAG(std::string, profile, "", "Profiles to test");

int main(int argc, char **argv) {
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

  ::testing::AddGlobalTestEnvironment(new tea::Environment);
  auto res = RUN_ALL_TESTS();
  if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
    arrow::fs::FinalizeS3().ok();
  }
  return res;
}
