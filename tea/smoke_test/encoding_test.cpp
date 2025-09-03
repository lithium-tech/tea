#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class EncodingTest : public TeaTest {};

TEST_F(EncodingTest, JsonTrivial) {
  std::string str1 = "aa \xd0\x93 bb";
  std::string str2 = "aa \xd0\x94 cc";
  std::string json1 = "{\"field1\": \"" + str1 + "\", \"field2\": \"q\"}";
  std::string json2 = "{\"field1\": \"" + str2 + "\", \"field2\": 122}";
  auto column = MakeJsonColumn("col1", 1, std::vector<std::string*>{nullptr, &json1, &json2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "json"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::Query("SELECT col1->>'field1' AS col1 FROM " + kDefaultTableName).Run(*conn_));
  auto expected_utf8 = pq::ScanResult({"col1"}, {{""}, {"aa Г bb"}, {"aa Д cc"}});
  bool equality_in_utf8 = result == expected_utf8;

  auto expected_cp1251 = pq::ScanResult({"col1"}, {{""}, {"aa \xc3 bb"}, {"aa \xc4 cc"}});
  bool equality_in_cp1251 = result == expected_cp1251;

  ASSERT_TRUE(equality_in_utf8 || equality_in_cp1251);
}

TEST_F(EncodingTest, JsonWithEscaping) {
  std::string str1 = "aa \\u0413 bb";
  std::string str2 = "aa \\u0414 cc";
  std::string json1 = "{\"field1\": \"" + str1 + "\", \"field2\": \"q\"}";
  std::string json2 = "{\"field1\": \"" + str2 + "\", \"field2\": 122}";
  auto column = MakeJsonColumn("col1", 1, std::vector<std::string*>{nullptr, &json1, &json2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "json"}}));

  std::cerr << json1 << std::endl << json2 << std::endl;
  ASSIGN_OR_FAIL(pq::ScanResult result,
                 pq::Query("SELECT col1->>'field1' AS col1 FROM " + kDefaultTableName).Run(*conn_));
  auto expected_utf8 = pq::ScanResult({"col1"}, {{""}, {"aa Г bb"}, {"aa Д cc"}});
  bool equality_in_utf8 = result == expected_utf8;

  auto expected_cp1251 = pq::ScanResult({"col1"}, {{""}, {"aa \xc3 bb"}, {"aa \xc4 cc"}});
  bool equality_in_cp1251 = result == expected_cp1251;

  ASSERT_TRUE(equality_in_utf8 || equality_in_cp1251);
}

}  // namespace
}  // namespace tea
