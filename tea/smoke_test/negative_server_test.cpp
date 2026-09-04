#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include "arrow/status.h"
#include "gtest/gtest.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/spark_generated_test_base.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/metadata.h"

namespace tea {

namespace {

class NegativeServer : public TeaTest {};
}  // namespace

class Defer {
 public:
  explicit Defer(const std::function<void()>& func) : func_(func) {}

  ~Defer() { func_(); }

 private:
  std::function<void()> func_;
};

TEST_F(NegativeServer, NoRedis) {
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

TEST_F(NegativeServer, NoHMS) {
  auto prev_profile = Environment::GetProfile();
  Environment::SetProfile("broken_hms");
  [[maybe_unused]] auto change_profile_defer = Defer([&prev_profile] { Environment::SetProfile(prev_profile); });

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
  if (Environment::GetTableType() != TestTableType::kExternal) {
    EXPECT_EQ(res.status().message().substr(0, 58), "SELECT failed: ERROR:  Tea error: No correct HMS endpoints");
  } else {
    EXPECT_EQ(res.status().message().substr(0, 107),
              "SELECT failed: ERROR:  Tea error: Combination external table + iceberg access + no samovar is not "
              "supported");
  }
}

TEST_F(NegativeServer, NoSuchTable) {
  if (Environment::GetProfile() != "samovar" || Environment::GetTableType() != TestTableType::kExternal) {
    return;
  }

  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8});
  auto column2 = MakeInt64Column("col2", 2, OptionalVector<int64_t>{1, 2, 3, 1, 2, 3, 1, 2, 3});
  ASSIGN_OR_FAIL(
      auto data_path,
      state_->WriteFile({column1, column2}, IFileWriter::Hints{.row_group_sizes = std::vector<size_t>{3, 3, 3}}));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"},
                                                  GreenplumColumnInfo{.name = "col2", .type = "int8"}},
                                                 kDefaultTableName));

  Environment::GetHiveMetastoreClient().ValueOrDie()->DropTable("test-tmp-db", kDefaultTableName);

  auto query_start = std::chrono::steady_clock::now().time_since_epoch();

  auto res = pq::TableScanQuery(kDefaultTableName, "col1").SetWhere("col1 != 4").Run(*conn_);

  ASSERT_FALSE(res.ok());
  EXPECT_TRUE(res.status().message().find("service has thrown: NoSuchObjectException") != std::string::npos)
      << res.status().message();

  auto query_end = std::chrono::steady_clock::now().time_since_epoch();

  auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start).count();
  EXPECT_LE(total_duration, 1000);
}

TEST_F(NegativeServer, NoLocationOptions) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    return;
  }

  const char* sql =
      "CREATE FOREIGN TABLE test_no_options (id int8, val text) "
      "SERVER tea_server";

  PGresult* res = PQexec(conn_->Ptr(), sql);

  ASSERT_NE(res, nullptr);
  ExecStatusType status = PQresultStatus(res);

  EXPECT_EQ(status, PGRES_FATAL_ERROR) << "Server should reject table creation without any options.";

  const char* err_msg = PQresultErrorMessage(res);
  EXPECT_TRUE(strstr(err_msg, "missing required option \"location\"") != nullptr)
      << "Error message should mention missing 'location' option. Got: " << err_msg;

  PQclear(res);
}

TEST_F(NegativeServer, EmptyLocationValue) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    return;
  }

  const char* sql =
      "CREATE FOREIGN TABLE test_empty_location (id int8, val text) "
      "SERVER tea_server OPTIONS (location '');";

  PGresult* res = PQexec(conn_->Ptr(), sql);

  ASSERT_NE(res, nullptr);
  ExecStatusType status = PQresultStatus(res);

  EXPECT_EQ(status, PGRES_FATAL_ERROR);

  const char* err_msg = PQresultErrorMessage(res);
  EXPECT_TRUE(strstr(err_msg, "option \"location\" value cannot be empty") != nullptr)
      << "Error message should mention empty value. Got: " << err_msg;

  PQclear(res);
}

TEST_F(NegativeServer, InvalidLocationOption) {
  if (Environment::GetTableType() == TestTableType::kExternal) {
    return;
  }

  const char* sql =
      "CREATE FOREIGN TABLE test_invalid_option (id int8, val text) "
      "SERVER tea_server OPTIONS (unknown_param 'value');";

  PGresult* res = PQexec(conn_->Ptr(), sql);

  ASSERT_NE(res, nullptr);
  ExecStatusType status = PQresultStatus(res);

  EXPECT_EQ(status, PGRES_FATAL_ERROR);

  const char* err_msg = PQresultErrorMessage(res);
  EXPECT_TRUE(strstr(err_msg, "invalid option \"unknown_param\"") != nullptr)
      << "Error message should mention invalid option. Got: " << err_msg;

  PQclear(res);
}

TEST_F(OtherEngineGeneratedTable, NoS3) {
  if (Environment::GetProfile() != "") {
    return;
  }
  auto prev_profile = Environment::GetProfile();
  Environment::SetProfile("broken_s3");
  [[maybe_unused]] auto change_profile_defer = Defer([&prev_profile] { Environment::SetProfile(prev_profile); });

  CreateTable("gperov", "test",
              std::vector<GreenplumColumnInfo>{GreenplumColumnInfo{.name = "a", .type = "int8"},
                                               GreenplumColumnInfo{.name = "b", .type = "int8"}});
  auto res = pq::TableScanQuery(kDefaultTableName).Run(*conn_);
  EXPECT_FALSE(res.ok());
  EXPECT_EQ(res.status().message().substr(0, 66), "SELECT failed: ERROR:  Tea error: When reading information for key");
}

class WrongTeapotMetadataWriter : public TeapotMetadataWriter {
 public:
  WrongTeapotMetadataWriter() : TeapotMetadataWriter(kDefaultTableName) {}

  arrow::Result<Location> Finalize() override {
    auto teapot_ptr = Environment::GetTeapotPtr();
    return Location(TeapotLocation("db", kDefaultTableName, "iamnotteapot", teapot_ptr->GetPort(),
                                   Options{.profile = Environment::GetProfile()}));
  }
};

class WrongTeapotMetadataWriterBuilder : public IMetadataWriterBuilder {
 public:
  explicit WrongTeapotMetadataWriterBuilder(std::shared_ptr<WrongTeapotMetadataWriter> instance)
      : instance_(instance) {}

  std::shared_ptr<IMetadataWriter> Build(const TableName& table_name) override { return instance_; }

 private:
  std::shared_ptr<WrongTeapotMetadataWriter> instance_;
};

}  // namespace tea
