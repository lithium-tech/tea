#include <string>
#include <unordered_map>

#include "arrow/status.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"

#include "tea/common/config.h"

namespace tea {

TEST(ProfileToTables, Trivial) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-tables": {
        "samovar": ["a"]
    }
})__";

  ASSIGN_OR_FAIL(auto result, GetTableToProfileMapping(std::string(kTestJsonConfig)));

  std::unordered_map<std::string, std::string> expected = {{"a", "samovar"}};
  EXPECT_EQ(result, expected);
}

TEST(ProfileToTables, Empty) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-tables": {}
})__";

  ASSIGN_OR_FAIL(auto result, GetTableToProfileMapping(std::string(kTestJsonConfig)));

  std::unordered_map<std::string, std::string> expected;
  EXPECT_EQ(result, expected);
}

TEST(ProfileToTables, IncorrectJson) {
  const std::string_view kTestJsonConfig = R"__({
   qwe{{}{}{}{}ad}
})__";

  auto maybe_result = GetTableToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(), "Profile-to-table parsing error: not a valid JSON");
}

TEST(ProfileToTables, RootIsNotAnObject) {
  const std::string_view kTestJsonConfig = R"__(
   "q"
)__";

  auto maybe_result = GetTableToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(), "Profile-to-table parsing error: root is not an object");
}

TEST(ProfileToTables, MissingRootField) {
  const std::string_view kTestJsonConfig = R"__({
    "a": "b"
})__";

  auto maybe_result = GetTableToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(),
            "Profile-to-table parsing error: field 'profile-to-tables' is expected but not found");
}

TEST(ProfileToTables, ValueIsNotAnArray) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-tables": {
      "samovar": "a,b,c"
    }
})__";

  auto maybe_result = GetTableToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(),
            "Profile-to-table parsing error: value for profile 'samovar' is not an array");
}

TEST(ProfileToTables, ElementIsNotAString) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-tables": {
      "samovar": ["a", "b", 123]
    }
})__";

  auto maybe_result = GetTableToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(),
            "Profile-to-table parsing error: element for key 'samovar' is not a string");
}

TEST(ProfileToTables, OneTableMultipleProfiles) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-tables": {
      "samovar": ["a", "c", "e", "g", "h1"],
      "teapot": ["a", "b", "e", "f", "h2"],
      "other": ["a", "b", "c", "d", "h3"]
    }
})__";

  ASSIGN_OR_FAIL(auto result, GetTableToProfileMapping(std::string(kTestJsonConfig)));

  std::unordered_map<std::string, std::string> expected = {{"g", "samovar"},  {"d", "other"},   {"f", "teapot"},
                                                           {"h1", "samovar"}, {"h2", "teapot"}, {"h3", "other"}};
  EXPECT_EQ(result, expected);
}

}  // namespace tea
