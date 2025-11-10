#include <string>

#include "arrow/status.h"
#include "arrow/util/decimal.h"
#include "gtest/gtest.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

#ifdef __SIZEOF_INT128__
using Int128 = __int128;
using UInt128 = unsigned __int128;
#else
#error Native support for __int128 is required
#endif

std::string Int128ToString(Int128 value) { return std::string(reinterpret_cast<const char*>(&value), sizeof(value)); }

std::string Int128ToStringBigEndian(Int128 value) {
  auto result = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
  auto decimal =
      arrow::Decimal128::FromBigEndian(reinterpret_cast<uint8_t*>(result.data()), sizeof(Int128)).ValueOrDie();
  return std::string(reinterpret_cast<const char*>(decimal.native_endian_bytes()), sizeof(Int128));
}

parquet::ByteArray ToByteArray(std::string_view s) {
  return parquet::ByteArray(s.size(), reinterpret_cast<const uint8_t*>(s.data()));
}

class TypesTest : public TeaTest {};

TEST_F(TypesTest, Boolean) {
  auto column = MakeBoolColumn("col1", 1, OptionalVector<bool>{std::nullopt, false, true});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "bool"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"f"}, {"t"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, BooleanArray) {
  auto column = MakeBoolColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<bool>{std::nullopt, false, true}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "bool[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,f,t}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Smallint) {
  auto column = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int2"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"-2"}, {"2"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, SmallintArray) {
  auto column = MakeInt16Column("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int32_t>{std::nullopt, -2, 2}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int2[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,-2,2}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Int) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"-2"}, {"2"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, IntArray) {
  auto column = MakeInt32Column("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int32_t>{std::nullopt, -2, 2}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,-2,2}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Bigint) {
  auto column = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"-2"}, {"2"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, BigintArray) {
  auto column = MakeInt64Column("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int64_t>{std::nullopt, -2, 2}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int8[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,-2,2}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Bytea) {
  std::string str = "qwe";
  auto column = MakeBinaryColumn("col1", 1, std::vector<std::string*>{nullptr, &str});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "bytea"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));

#if PG_VERSION_MAJOR >= 9
  auto expected = pq::ScanResult({"col1"}, {{""}, {"\\x717765"}});
#else
  auto expected = pq::ScanResult({"col1"}, {{""}, {"qwe"}});
#endif

  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, ByteaNonUtf8) {
  std::string str("\x00\xff\xff\x01\xfe\x02\x00\x00\xff", 9);
  auto column = MakeBinaryColumn("col1", 1, std::vector<std::string*>{nullptr, &str});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "bytea"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));

#if PG_VERSION_MAJOR >= 9
  auto expected = pq::ScanResult({"col1"}, {{""}, {"\\x00ffff01fe020000ff"}});
#else
  auto expected = pq::ScanResult({"col1"}, {{""}, {"\\000\\377\\377\\001\\376\\002\\000\\000\\377"}});
#endif

  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, ByteaArray) {
  std::string str1 = "qwe";
  std::string str2 = "<longer string>";
  auto column = MakeBinaryColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data =
      ArrayContainer{.arrays = {OptionalVector<parquet::ByteArray>{
                         std::nullopt, parquet::ByteArray(str1.size(), reinterpret_cast<const uint8_t*>(str1.data())),
                         parquet::ByteArray(str2.size(), reinterpret_cast<const uint8_t*>(str2.data()))}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "bytea[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));

#if PG_VERSION_MAJOR >= 9
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,\"\\\\x717765\",\"\\\\x3c6c6f6e67657220737472696e673e\"}"}});
#else
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,qwe,\"<longer string>\"}"}});
#endif

  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Text) {
  std::string str = "qwe";
  auto column = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"qwe"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, TextArray) {
  std::string str1 = "qwe";
  std::string str2 = "<longer string>";
  auto column = MakeStringColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data =
      ArrayContainer{.arrays = {OptionalVector<parquet::ByteArray>{
                         std::nullopt, parquet::ByteArray(str1.size(), reinterpret_cast<const uint8_t*>(str1.data())),
                         parquet::ByteArray(str2.size(), reinterpret_cast<const uint8_t*>(str2.data()))}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "text[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,qwe,\"<longer string>\"}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Json) {
  std::string json1 = "{\"field1\": \"asd\", \"field2\": 12}";
  std::string json2 = "{\"field1\": \"asd2\", \"field2\": 122}";
  auto column = MakeJsonColumn("col1", 1, std::vector<std::string*>{nullptr, &json1, &json2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "json"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {json1}, {json2}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, JsonArray) {
  auto column = MakeJsonColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{
      .arrays = {OptionalVector<parquet::ByteArray>{std::nullopt, ToByteArray(R"("qwe")"),
                                                    ToByteArray(R"({"int_value"=1,"string_value"="value"})")}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "json[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{R"({NULL,"\"qwe\"","{\"int_value\"=1,\"string_value\"=\"value\"}"})"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Float4) {
  auto column = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2, 3.77});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "float4"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"2"}, {"3.77"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, FloatEvolution) {
  {
    auto column = iceberg::MakeDoubleColumn("col1", 1, OptionalVector<double>{15, 114878});
    ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
    ASSERT_OK(state_->AddDataFiles({file_path}));
  }
  {
    auto column = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, 2, 3.75});
    ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
    ASSERT_OK(state_->AddDataFiles({file_path}));
  }
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "float8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"2"}, {"3.75"}, {"15"}, {"114878"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Float4Array) {
  auto column = MakeFloatColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<float>{std::nullopt, 2, 3.77}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "float4[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,2,3.77}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Float8) {
  auto column = MakeDoubleColumn("col1", 1, OptionalVector<double>{std::nullopt, 2, 3.77});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "float8"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"2"}, {"3.77"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Float8Array) {
  auto column = MakeDoubleColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<double>{std::nullopt, 2, 3.77}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "float8[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,2,3.77}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Date) {
  auto column = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 1, 17});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "date"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"1970-01-02"}, {"1970-01-18"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, DateArray) {
  auto column = MakeDateColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int32_t>{std::nullopt, 1, 17}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "date[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,1970-01-02,1970-01-18}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Time) {
  auto column = MakeTimeColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 1, 123});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "time"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"00:00:00.000001"}, {"00:00:00.000123"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, TimeArray) {
  auto column = MakeTimeColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int64_t>{std::nullopt, 1, 123}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "time[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,00:00:00.000001,00:00:00.000123}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Timestamp) {
  auto column = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "timestamp"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"1970-01-01 00:00:00.000123"}, {"1970-01-01 00:00:00.000321"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, TimestampArray) {
  auto column = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int64_t>{std::nullopt, 123, 321}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "timestamp[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{R"({NULL,"1970-01-01 00:00:00.000123","1970-01-01 00:00:00.000321"})"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, Timestamptz) {
  auto column = MakeTimestamptzColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "timestamptz"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected =
      pq::ScanResult({"col1"}, {{""}, {"1970-01-01 07:00:00.000123+07"}, {"1970-01-01 07:00:00.000321+07"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, TimestamptzArray) {
  auto column = MakeTimestamptzColumn("col1", 1, OptionalVector<int64_t>{});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int64_t>{std::nullopt, 123, 321}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSERT_OK(pq::SetTimeZone(*conn_, 7));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "timestamptz[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected =
      pq::ScanResult({"col1"}, {{R"({NULL,"1970-01-01 07:00:00.000123+07","1970-01-01 07:00:00.000321+07"})"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, NumericInt32) {
  auto column = MakeNumericColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, 123, 321}, 7, 2);
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"1.23"}, {"3.21"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, NumericInt64) {
  auto column = MakeNumericColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, 123, 321}, 7, 2);
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"1.23"}, {"3.21"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, NumericArrayInt64) {
  auto column = MakeNumericColumn("col1", 1, OptionalVector<int64_t>{}, 7, 2);
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data = ArrayContainer{.arrays = {OptionalVector<int64_t>{std::nullopt, 123, 321}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{"{NULL,1.23,3.21}"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, NumericByteArray) {
  std::string str1 = Int128ToStringBigEndian(123);
  std::string str2 = Int128ToStringBigEndian(321);
  auto column = MakeNumericColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2}, 7, 2);
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"1.23"}, {"3.21"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, NumericFixedLengthByteArray) {
  std::string str1 = Int128ToStringBigEndian(123);
  std::string str2 = Int128ToStringBigEndian(321);
  auto column = MakeNumericColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2}, 7, 2, 16);
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "numeric (7, 2)"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult({"col1"}, {{""}, {"1.23"}, {"3.21"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, UUID) {
  std::string str1 = Int128ToString(213);
  std::string str2 = Int128ToString(1);
  auto column = MakeUuidColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "uuid"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected = pq::ScanResult(
      {"col1"}, {{""}, {"d5000000-0000-0000-0000-000000000000"}, {"01000000-0000-0000-0000-000000000000"}});
  EXPECT_EQ(result, expected);
}

TEST_F(TypesTest, UUIDArray) {
  uint8_t value1[16] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff};
  uint8_t value2[16] = {0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11};
  auto column = MakeUuidColumn("col1", 1, {});
  column.info.repetition = parquet::Repetition::REPEATED;
  column.data =
      ArrayContainer{.arrays = {OptionalVector<parquet::FixedLenByteArray>{
                         std::nullopt, parquet::FixedLenByteArray{value1}, parquet::FixedLenByteArray{value2}}}};
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "uuid[]"}}));

  ASSIGN_OR_FAIL(pq::ScanResult result, pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_));
  auto expected =
      pq::ScanResult({"col1"}, {{"{NULL,00112233-4455-6677-8899-aabbccddeeff,11111111-1111-1111-1111-111111111111}"}});
  EXPECT_EQ(result, expected);
}

class NegativeTypesTest : public TypesTest {};

TEST_F(NegativeTypesTest, CharNotSupported) {
  std::string str1 = "abcdefghij";
  std::string str2 = "0123456789";
  auto column = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "char"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  EXPECT_TRUE(
      status.message().find(
          "Greenplum column 'col1' with type 'character(1)' (oid = 1042) has incompatible Iceberg type string") !=
      std::string::npos)
      << status.message();
}

TEST_F(NegativeTypesTest, VarcharNotSupported) {
  std::string str1 = "abcdefghij";
  std::string str2 = "0123456789";
  auto column = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "varchar (10)"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  EXPECT_TRUE(status.message().find("Greenplum column 'col1' with type 'character varying(10)' (oid = 1043) has "
                                    "incompatible Iceberg type string") != std::string::npos)
      << status.message();
}

TEST_F(NegativeTypesTest, GpStringIcebergInt) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 1, 2, 3});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "varchar (10)"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  EXPECT_TRUE(
      status.message().find(
          "Greenplum column 'col1' with type 'character varying(10)' (oid = 1043) has incompatible Iceberg type int") !=
      std::string::npos)
      << status.message();
}

TEST_F(NegativeTypesTest, GpIntIcebergString) {
  std::string str1 = "abcdefghij";
  std::string str2 = "0123456789";
  auto column = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2, &str1});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "int4"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  EXPECT_TRUE(status.message().find(
                  "Greenplum column 'col1' with type 'integer' (oid = 23) has incompatible Iceberg type string") !=
              std::string::npos)
      << status.message();
}

TEST_F(NegativeTypesTest, GpDecimalIcebergInt) {
  auto column = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, 1, 2, 3});
  ASSIGN_OR_FAIL(auto file_path, state_->WriteFile({column}));
  ASSERT_OK(state_->AddDataFiles({file_path}));
  ASSIGN_OR_FAIL(auto defer, state_->CreateTable({GreenplumColumnInfo{.name = "col1", .type = "numeric (6, 2)"}}));

  auto status = pq::Query("SELECT * FROM " + kDefaultTableName).Run(*conn_).status();
  ASSERT_FALSE(status.ok());

  EXPECT_TRUE(status.message().find(
                  "Greenplum column 'col1' with type 'numeric(6,2)' (oid = 1700) has incompatible Iceberg type int ") !=
              std::string::npos)
      << status.message();
}

}  // namespace
}  // namespace tea
