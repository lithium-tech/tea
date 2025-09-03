#include <string>

#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

class MismatchingTypesTest : public TeaTest {};

TEST_F(MismatchingTypesTest, BooleanInt) {
  auto column = MakeBoolColumn("col1", 1, OptionalVector<bool>{std::nullopt, false, true});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeInt32Column("col1", 1, OptionalVector<int>{std::nullopt, 2, 3});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "bool"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'boolean' (oid = 16) cannot be matched with arrow type int32; parquet file is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, BooleanBooleanArray) {
  auto column = MakeBoolColumn("col1", 1, OptionalVector<bool>{std::nullopt, false, true});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeBoolColumn("col1", 1, {});
  bad_column.info.repetition = parquet::Repetition::REPEATED;
  bad_column.data = ArrayContainer{.arrays = {OptionalVector<bool>{std::nullopt, false, true}}};
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "bool"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'boolean' (oid = 16) cannot be matched with arrow type list<element: bool>; parquet file "
      "is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, Int4Float4) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2, 3.77});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'integer' (oid = 23) cannot be matched with arrow type float; parquet file is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, Float4Int4) {
  auto column = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2, 3.77});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "float4"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'real' (oid = 700) cannot be matched with arrow type int32; parquet file is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, Int8Timestamp) {
  auto column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'bigint' (oid = 20) cannot be matched with arrow type timestamp[us]; parquet file is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, TimestampInt8) {
  auto column = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "timestamp"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'timestamp without time zone' (oid = 1114) cannot be matched with arrow type int64; "
      "parquet file is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, StringInt8) {
  std::string short_string = "a";
  std::string long_string = std::string(100, 'b');
  auto column = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &short_string, &long_string});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'text' (oid = 25) cannot be matched with arrow type int64; parquet file is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, TimestampTimestamptz) {
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));

  auto column = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeTimestamptzColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "timestamp"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'timestamp without time zone' (oid = 1114) cannot be matched with arrow type "
      "timestamp[us, "
      "tz=UTC]; parquet file is ";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

TEST_F(MismatchingTypesTest, TimestamptzTimestamp) {
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));

  auto column = MakeTimestamptzColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));

  auto bad_column = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto bad_file_path, state_->WriteFile({bad_column}));
  ASSERT_OK(state_->AddDataFiles({bad_file_path}));

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  std::string pattern_to_find =
      "Column 'col1' of type 'timestamp with time zone' (oid = 1184) cannot be matched with arrow type timestamp[us]; "
      "parquet file is";
  EXPECT_TRUE(status.message().find(pattern_to_find) != std::string::npos) << status.message();
}

}  // namespace
}  // namespace tea
