#include "tea/table/numeric_var.h"

#include <algorithm>
#include <initializer_list>

#include "gtest/gtest.h"

namespace tea {
namespace {

class NumericVarTest : public ::testing::Test {
 protected:
  void SetUp() override {
    quick_init_var(&var_);
    i64_ = 0;
    i128_ = 0;
  }

  void AssignVar(int weight, int sign, int dscale, std::initializer_list<NumericDigit> digits) {
    init_var(&var_, NUMERIC_LOCAL_NMAX);
    var_.ndigits = digits.size();
    var_.weight = weight;
    var_.sign = sign;
    var_.dscale = dscale;
    std::copy(digits.begin(), digits.end(), var_.digits);
  }

  NumericVar var_;
  int64_t i64_;
  Int128 i128_;
};

TEST_F(NumericVarTest, FromZero64) {
  Int64ToNumericVar(0, 0, &var_);
  EXPECT_EQ(var_.ndigits, 0);
  EXPECT_EQ(var_.weight, 0);
  EXPECT_EQ(var_.dscale, 0);
}

TEST_F(NumericVarTest, FromNegative1) {
  Int64ToNumericVar(-1, 0, &var_);
  EXPECT_EQ(var_.ndigits, 1);
  EXPECT_EQ(var_.weight, 0);
  EXPECT_EQ(var_.dscale, 0);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_NEG);
  EXPECT_EQ(var_.digits[0], 1);
}

TEST_F(NumericVarTest, FromPositive1) {
  Int64ToNumericVar(1, 0, &var_);
  EXPECT_EQ(var_.ndigits, 1);
  EXPECT_EQ(var_.weight, 0);
  EXPECT_EQ(var_.dscale, 0);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_POS);
  EXPECT_EQ(var_.digits[0], 1);
}

TEST_F(NumericVarTest, FromScale1) {
  Int64ToNumericVar(1, 1, &var_);
  EXPECT_EQ(var_.ndigits, 1);
  EXPECT_EQ(var_.weight, -1);
  EXPECT_EQ(var_.dscale, 1);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_POS);
  EXPECT_EQ(var_.digits[0], 1000);
}

TEST_F(NumericVarTest, FromScale2) {
  Int64ToNumericVar(1, 2, &var_);
  EXPECT_EQ(var_.ndigits, 1);
  EXPECT_EQ(var_.weight, -1);
  EXPECT_EQ(var_.dscale, 2);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_POS);
  EXPECT_EQ(var_.digits[0], 100);
}

TEST_F(NumericVarTest, FromScale3) {
  Int64ToNumericVar(1, 3, &var_);
  EXPECT_EQ(var_.ndigits, 1);
  EXPECT_EQ(var_.weight, -1);
  EXPECT_EQ(var_.dscale, 3);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_POS);
  EXPECT_EQ(var_.digits[0], 10);
}

TEST_F(NumericVarTest, FromScale4) {
  Int64ToNumericVar(1, 4, &var_);
  EXPECT_EQ(var_.ndigits, 1);
  EXPECT_EQ(var_.weight, -1);
  EXPECT_EQ(var_.dscale, 4);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_POS);
  EXPECT_EQ(var_.digits[0], 1);
}

TEST_F(NumericVarTest, FromScale5) {
  Int64ToNumericVar(1, 5, &var_);
  EXPECT_EQ(var_.ndigits, 1);
  EXPECT_EQ(var_.weight, -2);
  EXPECT_EQ(var_.dscale, 5);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_POS);
  EXPECT_EQ(var_.digits[0], 1000);
}

TEST_F(NumericVarTest, FromTwoNDigits) {
  Int64ToNumericVar(12345, 2, &var_);
  EXPECT_EQ(var_.ndigits, 2);
  EXPECT_EQ(var_.weight, 0);
  EXPECT_EQ(var_.dscale, 2);
  EXPECT_EQ(var_.sign, TEA_NUMERIC_POS);
  EXPECT_EQ(var_.digits[0], 123);
  EXPECT_EQ(var_.digits[1], 4500);
}

TEST_F(NumericVarTest, To0) {
  AssignVar(0, TEA_NUMERIC_POS, 0, {});
  ASSERT_TRUE(NumericVarToInt64(&var_, 0, &i64_));
  EXPECT_EQ(i64_, 0);
}

TEST_F(NumericVarTest, To1) {
  AssignVar(0, TEA_NUMERIC_POS, 0, {1});
  ASSERT_TRUE(NumericVarToInt64(&var_, 0, &i64_));
  EXPECT_EQ(i64_, 1);
}

TEST_F(NumericVarTest, ToScale1) {
  AssignVar(-1, TEA_NUMERIC_POS, 1, {1000});
  ASSERT_TRUE(NumericVarToInt64(&var_, 1, &i64_));
  EXPECT_EQ(i64_, 1);
}

TEST_F(NumericVarTest, ToScale2) {
  AssignVar(-1, TEA_NUMERIC_POS, 2, {100});
  ASSERT_TRUE(NumericVarToInt64(&var_, 2, &i64_));
  EXPECT_EQ(i64_, 1);
}

TEST_F(NumericVarTest, ToScale3) {
  AssignVar(-1, TEA_NUMERIC_POS, 3, {10});
  ASSERT_TRUE(NumericVarToInt64(&var_, 3, &i64_));
  EXPECT_EQ(i64_, 1);
}

TEST_F(NumericVarTest, ToScale4) {
  AssignVar(-1, TEA_NUMERIC_POS, 4, {1});
  ASSERT_TRUE(NumericVarToInt64(&var_, 4, &i64_));
  EXPECT_EQ(i64_, 1);
}

TEST_F(NumericVarTest, ToScale5) {
  AssignVar(-2, TEA_NUMERIC_POS, 5, {1000});
  ASSERT_TRUE(NumericVarToInt64(&var_, 5, &i64_));
  EXPECT_EQ(i64_, 1);
}

TEST_F(NumericVarTest, ToScale2TwoNDigits) {
  AssignVar(0, TEA_NUMERIC_POS, 2, {123, 4500});
  ASSERT_TRUE(NumericVarToInt64(&var_, 2, &i64_));
  EXPECT_EQ(i64_, 12345);
}

TEST_F(NumericVarTest, ToMaxDigitsInt64) {
  AssignVar(2, TEA_NUMERIC_POS, 9, {9, 8765, 4321, 1234, 5678, 9000});
  ASSERT_TRUE(NumericVarToInt64(&var_, 9, &i64_));
  EXPECT_EQ(i64_, 987654321123456789);
}

TEST_F(NumericVarTest, ToTrailingZeros) {
  AssignVar(2, TEA_NUMERIC_POS, 4, {9, 8765, 4321, 1234});
  ASSERT_TRUE(NumericVarToInt64(&var_, 9, &i64_));
  EXPECT_EQ(i64_, 987654321123400000);
}

TEST_F(NumericVarTest, To18DigitsInt128) {
  AssignVar(2, TEA_NUMERIC_POS, 9, {9, 8765, 4321, 1234, 5678, 9000});
  ASSERT_TRUE(NumericVarToInt128(&var_, 9, &i128_));
  EXPECT_EQ(i128_, 987654321123456789);
}

}  // namespace
}  // namespace tea
