#pragma once

#include <memory>
#include <optional>
#include <string>

#include "arrow/filesystem/s3fs.h"
#include "gtest/gtest.h"
#include "tools/hive_metastore_client.h"

#include "tea/smoke_test/pq.h"

namespace tea {

class MockTeapot;
class StatsState;

enum class TestTableType { kExternal, kForeign };
enum class MetadataType { kIceberg, kTeapot };

std::optional<TestTableType> TableTypeFromString(const std::string& str);
std::optional<MetadataType> MetadataTypeFromString(const std::string& str);

class Environment : public ::testing::Environment {
 public:
  // TODO(gmusya): release resources in TearDown
  virtual void SetUp();

  static std::shared_ptr<arrow::fs::S3FileSystem> GetS3Filesystem();
  static MockTeapot* GetTeapotPtr();
  static StatsState* GetStatsStatePtr();
  static arrow::Result<iceberg::HiveMetastoreClient*> GetHiveMetastoreClient();

  static pq::PGconnWrapper& GetConnWrapper();

  static TestTableType GetTableType();
  static MetadataType GetMetadataType();
  static const std::string& GetProfile();

  static void SetTableType(TestTableType table_type);
  static void SetMetadataType(MetadataType meta_type);
  static void SetProfile(const std::string& profile);
};

}  // namespace tea
