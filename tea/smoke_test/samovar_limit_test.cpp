#include <functional>
#include <string>
#include <utility>

#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/metadata.h"
#include "tea/util/defer.h"

namespace tea {
namespace {

class SamovarLimitTest : public TeaTest {
 protected:
  void SetUp() override {
    if (Environment::GetMetadataType() != MetadataType::kIceberg || Environment::GetProfile() != "samovar") {
      GTEST_SKIP() << "Skip test only for iceberg with Samovar profile";
    }
    TeaTest::SetUp();
  }
};

std::string MakeUnionAllQuery(const std::string& table_name, int scans_count) {
  std::string query = "SELECT count(*) FROM (SELECT col1 FROM " + table_name;
  for (int i = 1; i < scans_count; ++i) {
    query += " UNION ALL SELECT col1 FROM " + table_name;
  }
  return query + ") t";
}

Defer UseLimitedProfile() {
  auto prev_profile = Environment::GetProfile();
  Environment::SetProfile("iceberg_samovar_connection_limit");
  return Defer([prev_profile] { Environment::SetProfile(prev_profile); });
}

void CreateSingleRowTable(TestState& state) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{1});
  ASSIGN_OR_FAIL(auto file_path, state.WriteFile({column}));
  ASSERT_OK(state.AddDataFiles({file_path}));
}

constexpr int kMaxQuerySegmentScans = 20;

TEST_F(SamovarLimitTest, SimpleQueriesWork) {
  auto change_profile_defer = UseLimitedProfile();

  CreateSingleRowTable(*state_);
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  for (int i = 0; i < kMaxQuerySegmentScans + 1; ++i) {
    ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query(MakeUnionAllQuery(kDefaultTableName, 1)).Run(*conn_));
    ASSERT_EQ(result, pq::ScanResult({"count"}, {{"1"}}));
  }
}

TEST_F(SamovarLimitTest, ClientsLimitExceeded) {
  auto change_profile_defer = UseLimitedProfile();

  CreateSingleRowTable(*state_);
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  const int segments_count = pq::GetSegmentsCount(*conn_);
  const int scans_count = kMaxQuerySegmentScans / segments_count + 2;
  auto status = pq::Query(MakeUnionAllQuery(kDefaultTableName, scans_count)).Run(*conn_).status();

  ASSERT_FALSE(status.ok());
  EXPECT_TRUE(status.message().find("Query exceeds Samovar scan limit") != std::string::npos) << status.message();
}

}  // namespace
}  // namespace tea
