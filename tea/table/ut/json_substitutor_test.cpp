#include "tea/table/json_substitutor.h"

#include <chrono>
#include <filesystem>
#include <fstream>

#include "gtest/gtest.h"

namespace tea {

namespace {

TEST(JsonSubstitutorTest, Identity) {
  std::string data = "{\"key\": \"value\"}";
  std::string result = SubstituteAsciiCp1251(data);
  EXPECT_EQ(data, result);
}

TEST(JsonSubstitutorTest, ChangesEscapedUnicode) {
  std::string data = "{\"key\": \"val\\u0413e\"}";
  std::string result = SubstituteAsciiCp1251(data);

  std::string expected_result = "{\"key\": \"valГe\"}";
  EXPECT_EQ(expected_result, result);
}

TEST(JsonSubstitutorTest, ChangesEscapedUnicodeWithThreeChars) {
  std::string data = "{\"key\": \"val\\u2122e\"}";
  std::string result = SubstituteAsciiCp1251(data);

  std::string expected_result = "{\"key\": \"val™e\"}";
  EXPECT_EQ(expected_result, result);
}

TEST(JsonSubstitutorTest, DoesNotChangeEscapedAscii) {
  std::string data = "{\"key\": \"val\\u0043e\"}";
  std::string result = SubstituteAsciiCp1251(data);

  EXPECT_EQ(data, result);
}

TEST(JsonSubstitutorTest, FakeEscaping) {
  std::string data = "{\"key\": \"val\\\\u0413e\"}";
  std::string result = SubstituteAsciiCp1251(data);

  EXPECT_EQ(data, result);
}

TEST(JsonSubstitutorTest, ReplaceUnknownUnicode) {
  std::string data = "{\"key\": \"val\\u5273e\"}";
  std::string result = SubstituteAsciiCp1251(data);

  std::string expected_result = "{\"key\": \"val?e\"}";
  EXPECT_EQ(expected_result, result);
}

TEST(JsonSubstitutorTest, IncorrectEscaping) {
  std::string data = "{\"key\": \"val\\u527ze\"}";
  EXPECT_ANY_THROW(SubstituteAsciiCp1251(data));
}

TEST(JsonSubstitutorTest, IncorrectEscaping2) {
  std::string data = "{\"key\": \"val\\u5\"}";
  EXPECT_ANY_THROW(SubstituteAsciiCp1251(data));
}

}  // namespace
}  // namespace tea
