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
  std::unordered_map<std::string, std::string> m_server_options;

  config = ConfigSource::GetTableConfig(m_server_options, "tea://special://empty");
  EXPECT_TRUE(std::holds_alternative<EmptyTable>(config.source));

  config = ConfigSource::GetTableConfig(m_server_options, "tea://file:///root/subdir/file");
  EXPECT_THAT(config.source, testing::VariantWith<FileTable>(FileTable{"file:///root/subdir/file"}));

  config = ConfigSource::GetTableConfig(m_server_options, "tea://s3://bucket/prefix/file");
  EXPECT_THAT(config.source, testing::VariantWith<FileTable>(FileTable{"s3://bucket/prefix/file"}));

  config = ConfigSource::GetTableConfig(m_server_options, "tea://teapot://table.id");
  EXPECT_THAT(config.source, testing::VariantWith<TeapotTable>(TeapotTable{.table_id = {"table", "id"}}));

  config = ConfigSource::GetTableConfig(m_server_options, "tea://teapot://host:1234/table.id");
  EXPECT_THAT(config.source, testing::VariantWith<TeapotTable>(TeapotTable{.table_id = {"table", "id"}}));
  EXPECT_EQ(config.config.teapot.location, "host:1234");

  config = ConfigSource::GetTableConfig(m_server_options, "tea://table.id");
  EXPECT_THAT(config.source, testing::VariantWith<TeapotTable>(TeapotTable{.table_id = {"table", "id"}}));

  config = ConfigSource::GetTableConfig(m_server_options, "tea://table.id?profile=override");
  EXPECT_THAT(config.source, testing::VariantWith<TeapotTable>(TeapotTable{.table_id = {"table", "id"}}));
  EXPECT_EQ(config.config.s3.access_key, "OVERRIDE");

  config = ConfigSource::GetTableConfig(m_server_options, "tea://iceberg://table.id");
  EXPECT_THAT(config.source, testing::VariantWith<IcebergTable>(IcebergTable{.table_id = {"table", "id"}}));

  config = ConfigSource::GetTableConfig(m_server_options, "tea://iceberg://table.id?snapshot_id=123");
  EXPECT_THAT(config.source, testing::VariantWith<IcebergTable>(IcebergTable{.table_id = {"table", "id"}}));
  ASSERT_TRUE(std::holds_alternative<Snapshot>(config.snapshot_ref));
  EXPECT_EQ(std::get<Snapshot>(config.snapshot_ref).snapshot_id, 123);

  config = ConfigSource::GetTableConfig(m_server_options, "tea://iceberg://table.id?branch=test-branch");
  EXPECT_THAT(config.source, testing::VariantWith<IcebergTable>(IcebergTable{.table_id = {"table", "id"}}));
  ASSERT_TRUE(std::holds_alternative<Branch>(config.snapshot_ref));
  EXPECT_EQ(std::get<Branch>(config.snapshot_ref).name, "test-branch");
}

TEST_F(ConfigSourceTest, InvalidUrl) {
  std::unordered_map<std::string, std::string> m_server_options;
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig(m_server_options, "special://empty"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig(m_server_options, "tea://special://unrecognized_special"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig(m_server_options, "tea://hdfs://unsupported"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig(m_server_options, "tea://teapot://invalid_table_id"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig(m_server_options, "tea://iceberg://invalid_table_id"));
  EXPECT_ANY_THROW(ConfigSource::GetTableConfig(m_server_options, "tea://iceberg://table.id?snapshot_id=abc"));
  EXPECT_ANY_THROW(
      ConfigSource::GetTableConfig(m_server_options, "tea://iceberg://table.id?snapshot_id=123&branch=test-branch"));
}

TEST(ConfigSourceTestWithoutConfig, ServerOptions) {
  std::unordered_map<std::string, std::string> m_server_options = {
    {"read_config_file", "false"},
    {"s3_access_key", "ak"},
    {"s3_secret_key", "sk"},
    {"s3_endpoint_override", "storage.yandexcloud.net"},
    {"s3_scheme", "http"},
    {"catalog_type", "nessie"},
#if USE_REST
    {"catalog_rest_url", "http://127.0.0.1:8181/catalog"},
    {"catalog_rest_warehouse_id", "91dc12d2-534d-11f1-9109-73b91866a831"}
#endif
  };

  TableConfig tc = ConfigSource::GetTableConfig(m_server_options, "tea://iceberg://table.id");
  Config& config = tc.config;
  EXPECT_EQ(config.s3.access_key, "ak");
  EXPECT_EQ(config.s3.secret_key, "sk");
  EXPECT_EQ(config.s3.endpoint_override, "storage.yandexcloud.net");
  EXPECT_EQ(config.s3.scheme, "http");
  EXPECT_EQ(config.catalog.type, CatalogConfig::CatalogType::kNessie);
#if USE_REST
  EXPECT_EQ(config.catalog.rest_url, "http://127.0.0.1:8181/catalog");
  EXPECT_EQ(config.catalog.rest_warehouse_id, "91dc12d2-534d-11f1-9109-73b91866a831");
#endif
}

}  // namespace
}  // namespace tea
