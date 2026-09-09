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

TEST(UsernameToProfile, Trivial) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-username": {
        "someprofile": ["name1"]
    }
})__";

  ASSIGN_OR_FAIL(auto result, GetUsernameToProfileMapping(std::string(kTestJsonConfig)));

  std::unordered_map<std::string, std::string> expected = {{"name1", "someprofile"}};
  EXPECT_EQ(result, expected);
}

TEST(UsernameToProfile, Empty) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-username": {}
})__";

  ASSIGN_OR_FAIL(auto result, GetUsernameToProfileMapping(std::string(kTestJsonConfig)));

  std::unordered_map<std::string, std::string> expected;
  EXPECT_EQ(result, expected);
}

TEST(UsernameToProfile, MissingRootFieldIsNotAnError) {
  const std::string_view kTestJsonConfig = R"__({
    "a": "b"
})__";

  ASSIGN_OR_FAIL(auto result, GetUsernameToProfileMapping(std::string(kTestJsonConfig)));

  std::unordered_map<std::string, std::string> expected;
  EXPECT_EQ(result, expected);
}

TEST(UsernameToProfile, IncorrectJson) {
  const std::string_view kTestJsonConfig = R"__({
   qwe{{}{}{}{}ad}
})__";

  auto maybe_result = GetUsernameToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(), "Profile-to-username parsing error: not a valid JSON");
}

TEST(UsernameToProfile, RootIsNotAnObject) {
  const std::string_view kTestJsonConfig = R"__(
   "q"
)__";

  auto maybe_result = GetUsernameToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(), "Profile-to-username parsing error: root is not an object");
}

TEST(UsernameToProfile, ValueIsNotAnArray) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-username": {
      "someprofile": "name1,name2"
    }
})__";

  auto maybe_result = GetUsernameToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(),
            "Profile-to-username parsing error: value for profile 'someprofile' is not an array");
}

TEST(UsernameToProfile, ElementIsNotAString) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-username": {
      "someprofile": ["name1", "name2", 123]
    }
})__";

  auto maybe_result = GetUsernameToProfileMapping(std::string(kTestJsonConfig));
  ASSERT_NE(maybe_result.status(), arrow::Status::OK());

  EXPECT_EQ(maybe_result.status().message(),
            "Profile-to-username parsing error: element for key 'someprofile' is not a string");
}

TEST(UsernameToProfile, UsernameClaimedByMultipleProfiles) {
  const std::string_view kTestJsonConfig = R"__({
    "profile-to-username": {
      "someprofile1": ["name1", "name3", "name5", "name7", "shared1"],
      "someprofile2": ["name1", "name2", "name5", "name6", "shared2"],
      "someprofile3": ["name1", "name2", "name3", "name4", "shared3"]
    }
})__";

  ASSIGN_OR_FAIL(auto result, GetUsernameToProfileMapping(std::string(kTestJsonConfig)));

  std::unordered_map<std::string, std::string> expected = {{"name7", "someprofile1"},   {"name4", "someprofile3"},
                                                           {"name6", "someprofile2"},   {"shared1", "someprofile1"},
                                                           {"shared2", "someprofile2"}, {"shared3", "someprofile3"}};
  EXPECT_EQ(result, expected);
}

}  // namespace tea
