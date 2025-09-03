#pragma once

#include <string>
#include <utility>
#include <vector>

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/test_base.h"

namespace tea {

class OtherEngineGeneratedTable : public TeaTest {
 protected:
  void SetUp() override {
    if (Environment::GetMetadataType() != MetadataType::kIceberg) {
      GTEST_SKIP();
    }
    if (Environment::GetProfile() == "samovar_0" || Environment::GetProfile() == "samovar_1") {
      GTEST_SKIP();
    }
    TeaTest::SetUp();
  }

 public:
  void CreateTable(const std::string& hms_db_name, const std::string& hms_table_name,
                   std::vector<GreenplumColumnInfo> columns) {
    auto ice_loc = IcebergLocation(hms_db_name, hms_table_name, Options{.profile = Environment::GetProfile()});
    auto loc = Location(std::move(ice_loc));
    auto query = pq::CreateForeignTableQuery(columns, kDefaultTableName, loc);
    ASSIGN_OR_FAIL(auto defer, query.Run(*conn_));
    defer_.emplace(std::move(defer));
  }

 private:
  std::optional<pq::DropTableDefer> defer_;
};

}  // namespace tea
