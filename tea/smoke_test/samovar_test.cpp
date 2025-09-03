#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "arrow/status.h"
#include "gtest/gtest.h"

#include "tea/common/config.h"
#include "tea/samovar/network_layer/backoff.h"
#include "tea/samovar/network_layer/redis_client.h"
#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {

static constexpr const char* metadata_prefix = "/samovar_meta";
static constexpr const char* file_list_prefix = "/file_list";
static constexpr const char* checkpoint_prefix = "/checkpoint";

enum class CellType { kMetadataCell, kFileListCell, kCheckPointCell, kQueueCell };

CellType ClassifyCell(const std::string& cell) {
  if (cell.starts_with(metadata_prefix)) {
    return CellType::kMetadataCell;
  }
  if (cell.starts_with(file_list_prefix)) {
    return CellType::kFileListCell;
  }
  if (cell.starts_with(checkpoint_prefix)) {
    return CellType::kCheckPointCell;
  }
  return CellType::kQueueCell;
}

namespace {

class SamovarTest : public TeaTest {};
}  // namespace

class Defer {
 public:
  explicit Defer(const std::function<void()>& func) : func_(func) {}

  ~Defer() { func_(); }

 private:
  std::function<void()> func_;
};

TEST_F(SamovarTest, NoRedis) {
  if (Environment::GetProfile() != "samovar") {
    return;
  }
  Environment::SetProfile("broken_samovar");
  [[maybe_unused]] auto change_profile_defer = Defer([] { Environment::SetProfile("samovar"); });

  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{3, 3, 3}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}}));

  auto res = pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 != 4").Run(*conn_);
  EXPECT_FALSE(res.ok());
  EXPECT_EQ(res.status().message().substr(0, 59), "SELECT failed: ERROR:  Tea error: No available server redis");
}

TEST_F(SamovarTest, CheckQueues) {
  if (Environment::GetProfile() != "samovar") {
    return;
  }
  Environment::SetProfile("lateprocess_samovar");
  [[maybe_unused]] auto change_profile_defer = Defer([] { Environment::SetProfile("samovar"); });

  auto client = std::make_shared<samovar::SamovarRedisClient>(
      std::vector<Endpoint>{Endpoint{.host = "0.0.0.0", .port = samovar::kDefaultPort}},
      std::make_shared<samovar::NoBackoff>(5, nullptr), std::chrono::milliseconds(5000),
      std::chrono::milliseconds(5000));
  client->Clear();

  OptionalVector<int32_t> values;
  for (int i = 0; i < 8; ++i) {
    values.push_back(i);
  }
  auto column1 = MakeInt32Column("col1", 1, values);
  auto column2 = MakeInt32Column("col2", 2, values);
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column1, column2}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int4"}}));

  auto async_select_query = pq::AsyncTableScanQuery(kDefaultTableName);
  EXPECT_TRUE(async_select_query.Run(*this->conn_));
  const int max_retries = 30;
  std::vector<std::string> keys;
  for (int i = 0; i < max_retries; ++i) {
    keys = client->GetAllKeys();
    if (!keys.empty()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  EXPECT_FALSE(keys.empty());
  std::this_thread::sleep_for(std::chrono::seconds(1));

  for (const auto& key : keys) {
    auto type = ClassifyCell(key);
    switch (type) {
      case CellType::kCheckPointCell: {
        EXPECT_EQ(*client->GetCell(key), "3");
        break;
      }
      case CellType::kQueueCell: {
        EXPECT_EQ(client->GetQueueLen(key), 1);
        break;
      }
      default:
        continue;
    }
  }
  async_select_query.CancelQuery(*this->conn_);
}

}  // namespace tea
