#include "tea/common/config.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>

#include "arrow/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/scoped_temp_dir.h"

namespace tea {

namespace {
const char* const kTestJsonConfig = R"__(
{
    "profile-to-tables-path": "file://i-am-path",
    "common": {
        "s3": {
            "access_key": "EXAMPLE",
            "retry_max_attempts": 7
        },
        "limits": {
            "max_cpu_threads": 1,
            "max_io_threads": 2,
            "parquet_buffer_size": 1024
        },
        "experimental_features": {
            "filter_ignored_op_exprs": [7,1,412,5124,19292],
            "filter_ignored_func_exprs": [4]
        }
    },
    "profiles": {
        "override": {
            "s3": {
                "access_key": "OVERRIDE"
            }
        }
    }
}
)__";

TEST(JsonConfigTest, EnvNotSet) {
  unsetenv("GPHOME");
  EXPECT_FALSE(Config::GetJsonFilePath().ok());
}

TEST(JsonConfigTest, EnvSet) {
  setenv("GPHOME", "/gp/home", 1);
  EXPECT_EQ(Config::GetJsonFilePath(), arrow::Result<std::string>("/gp/home/tea/tea-config.json"));
}

TEST(JsonConfigTest, FileNotFound) {
  auto config = Config{};
  ASSERT_FALSE(config.FromJsonFile("/non/existing/file", std::nullopt).ok());
  EXPECT_EQ(config, Config{});
}

TEST(JsonConfigTest, ProfileToTablesPath) {
  Config config{};

  ASSERT_OK(config.FromJsonString(kTestJsonConfig, std::nullopt));
  EXPECT_EQ(config.profile_to_tables_path, "file://i-am-path");
}

TEST(JsonConfigTest, Limits) {
  Config config{};

  EXPECT_EQ(config.limits.max_cpu_threads, 1u);
  EXPECT_EQ(config.limits.max_io_threads, 1u);

  ASSERT_OK(config.FromJsonString(kTestJsonConfig, std::nullopt));
  EXPECT_EQ(config.limits.max_cpu_threads, 1u);
  EXPECT_EQ(config.limits.max_io_threads, 2u);
  EXPECT_EQ(config.limits.parquet_buffer_size, 1024u);
}

TEST(JsonConfigTest, ProfileOverride) {
  Config config{};
  ASSERT_OK(config.FromJsonString(kTestJsonConfig, std::nullopt, "override"));
  EXPECT_EQ(config.s3.access_key, "OVERRIDE");
  EXPECT_EQ(config.s3.retry_max_attempts, 7);
  EXPECT_EQ(config.s3.connect_timeout, std::chrono::milliseconds(1000));
}

TEST(JsonConfigTest, FilterIgnoredExprs) {
  Config config{};
  ASSERT_OK(config.FromJsonString(kTestJsonConfig, std::nullopt));
  EXPECT_EQ(config.features.filter_ignored_op_exprs, (std::vector<int>{7, 1, 412, 5124, 19292}));
  EXPECT_EQ(config.features.filter_ignored_func_exprs, (std::vector<int>{4}));
}

struct ConfigSourceTest : public testing::Test {
 public:
  void SetUp() override {
    setenv("GPHOME", dir_.path().native().c_str(), 1);
    std::filesystem::create_directory(dir_.path() / "tea");
    std::ofstream f(dir_.path() / "tea" / "tea-config.json");
    f << kTestJsonConfig;
  }

  iceberg::ScopedTempDir dir_;
};

TEST_F(ConfigSourceTest, TableTypes) {
  TableConfig config;

  config = ConfigSource::GetTableConfig("tea://special://empty");
  EXPECT_TRUE(std::holds_alternative<EmptyTable>(config.source));

  config = ConfigSource::GetTableConfig("tea://file:///root/subdir/file");
  EXPECT_THAT(config.source, testing::VariantWith<FileTable>(FileTable{"file:///root/subdir/file"}));

  config = ConfigSource::GetTableConfig("tea://s3://bucket/prefix/file");
  EXPECT_THAT(config.source, testing::VariantWith<FileTable>(FileTable{"s3://bucket/prefix/file"}));

  config = ConfigSource::GetTableConfig("tea://iceberg://table.id");
  EXPECT_THAT(config.source, testing::VariantWith<IcebergTable>(IcebergTable{.table_id = {"table", "id"}}));
}

TEST_F(ConfigSourceTest, InvalidUrl) {
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig("special://empty"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig("tea://special://unrecognized_special"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig("tea://hdfs://unsupported"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig("tea://teapot://invalid_table_id"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig("tea://iceberg://invalid_table_id"));
}

}  // namespace
}  // namespace tea
