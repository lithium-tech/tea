#include "tea/smoke_test/environment.h"

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/s3fs.h>

#include <memory>

#include "iceberg/test_utils/assertions.h"
#include "tools/hive_metastore_client.h"

#include "tea/smoke_test/mock_teapot.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/stats_state.h"

namespace tea {

std::optional<TestTableType> TableTypeFromString(const std::string& str) {
  if (str == "external") {
    return TestTableType::kExternal;
  } else if (str == "foreign") {
    return TestTableType::kForeign;
  } else {
    return std::nullopt;
  }
}

std::optional<MetadataType> MetadataTypeFromString(const std::string& str) {
  if (str == "teapot") {
    return MetadataType::kTeapot;
  } else if (str == "iceberg") {
    return MetadataType::kIceberg;
  } else {
    return std::nullopt;
  }
}

void Environment::SetUp() {
  GetTeapotPtr();
  GetConnWrapper();
  GetS3Filesystem();
  auto& wrapper = GetConnWrapper();
  ASSERT_OK(pq::DropTea(wrapper));
  ASSERT_OK(pq::CreateTea(wrapper));
}

MockTeapot* Environment::GetTeapotPtr() {
  static MockTeapot teapot;
  return &teapot;
}

static std::shared_ptr<iceberg::HiveMetastoreClient>& HiveMetastoreClientRef() {
  static std::shared_ptr<iceberg::HiveMetastoreClient> client = []() -> std::shared_ptr<iceberg::HiveMetastoreClient> {
    try {
      return std::make_shared<iceberg::HiveMetastoreClient>("localhost", 9090);
    } catch (...) {
      return nullptr;
    }
  }();
  return client;
}

arrow::Result<iceberg::HiveMetastoreClient*> Environment::GetHiveMetastoreClient() {
  auto client = HiveMetastoreClientRef();
  if (client) {
    return client.get();
  } else {
    return arrow::Status::ExecutionError("HiveMetastoreClient: failed to connect");
  }
}

pq::PGconnWrapper& Environment::GetConnWrapper() {
  static pq::PGconnWrapper wrapper(PQconnectdb("dbname = postgres"));
  return wrapper;
}

namespace {
arrow::Status InitializeS3IfNecessary() {
  if (!arrow::fs::IsS3Initialized()) {
    arrow::fs::S3GlobalOptions global_options{};
    global_options.log_level = arrow::fs::S3LogLevel::Fatal;
    return arrow::fs::InitializeS3(global_options);
  }
  return arrow::Status::OK();
}

arrow::Result<arrow::fs::S3Options> MakeS3Options() {
  ARROW_RETURN_NOT_OK(InitializeS3IfNecessary());

  auto options = arrow::fs::S3Options::FromAccessKey("minioadmin", "minioadmin");
  options.endpoint_override = "127.0.0.1:9000";
  options.scheme = "http";
  options.connect_timeout = 1;
  options.request_timeout = 3;
  options.allow_bucket_creation = true;
  options.allow_bucket_deletion = true;
  return options;
}

arrow::Result<std::shared_ptr<arrow::fs::S3FileSystem>> MakeS3FileSystem(arrow::fs::S3Options options) {
  setenv("AWS_EC2_METADATA_DISABLED", "true", 1);
  return arrow::fs::S3FileSystem::Make(options);
}
}  // namespace

std::shared_ptr<arrow::fs::S3FileSystem> Environment::GetS3Filesystem() {
  static std::shared_ptr<arrow::fs::S3FileSystem> fs = [&]() -> std::shared_ptr<arrow::fs::S3FileSystem> {
    auto maybe_options = MakeS3Options();
    if (!maybe_options.ok()) {
      std::cerr << maybe_options.status().ToString() << std::endl;
      return nullptr;
    }
    auto maybe_fs = MakeS3FileSystem(maybe_options.ValueUnsafe());
    if (!maybe_fs.ok()) {
      std::cerr << maybe_fs.status().ToString() << std::endl;
      return nullptr;
    }
    return maybe_fs.ValueUnsafe();
  }();
  return fs;
}

static TestTableType& TableTypeRef() {
#ifdef TEA_BUILD_FDW
  static TestTableType table = TestTableType::kForeign;
#else
  static TestTableType table = TestTableType::kExternal;
#endif
  return table;
}

static MetadataType& MetadataTypeRef() {
  static MetadataType meta_type = MetadataType::kTeapot;
  return meta_type;
}

static std::string& ProfileRef() {
  static std::string profile = "";
  return profile;
}

TestTableType Environment::GetTableType() { return TableTypeRef(); }

MetadataType Environment::GetMetadataType() { return MetadataTypeRef(); }

const std::string& Environment::GetProfile() { return ProfileRef(); }

void Environment::SetTableType(TestTableType table_type) { TableTypeRef() = table_type; }

void Environment::SetProfile(const std::string& profile) { ProfileRef() = profile; }

void Environment::SetMetadataType(MetadataType meta_type) { MetadataTypeRef() = meta_type; }

StatsState* Environment::GetStatsStatePtr() {
  static StatsState stats_state;
  return &stats_state;
}

}  // namespace tea
