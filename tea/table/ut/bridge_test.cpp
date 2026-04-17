#include "tea/table/bridge.h"

#include <string_view>

#include "absl/cleanup/cleanup.h"
#include "arrow/status.h"
#include "gtest/gtest.h"

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
}

#ifdef USE_ASSERT_CHECKING
bool assert_enabled = true;

void ExceptionalCondition(const char* conditionName, const char* errorType, const char* fileName, int lineNumber) {
  std::cerr << "conditionName = " << conditionName << '\n'
            << "errorType = " << errorType << '\n'
            << "fileName = " << fileName << '\n'
            << "lineNumber = " << lineNumber << '\n';
  abort();
}
#endif

namespace tea {
namespace {

std::string_view StringViewFromText(text* str) { return {VARDATA(str), VARSIZE_ANY_EXHDR(str)}; }

static_assert(JSONOID == 114, "ArrowToGpConverter assumes jsonoid == 114");

TEST(BridgeTest, IdentityConverter) {
  CharsetConverter converter = MakeIdentityConverter();
  text* res = converter.proc(converter.context, "abc", 3);
  ASSERT_NE(res, nullptr);
  absl::Cleanup res_cleanup = [res] { pfree(res); };
  EXPECT_EQ(StringViewFromText(res), "abc");
}

TEST(BridgeTest, IconvConverterNoConversion) {
  auto res = InitializeIconv(PG_UTF8);
  ASSERT_EQ(res.status().code(), arrow::StatusCode::OK);
  EXPECT_EQ(*res, nullptr);
}

TEST(BridgeTest, IconvConverterUnsupported) {
  auto res = InitializeIconv(PG_WIN1252);
  ASSERT_NE(res.status().code(), arrow::StatusCode::OK);
}

TEST(BridgeTest, IconvConverter) {
  auto res = InitializeIconv(PG_WIN1251);
  ASSERT_EQ(res.status().code(), arrow::StatusCode::OK);
  ASSERT_NE(*res, nullptr);
  absl::Cleanup res_cleanup = [res] { (void)FinalizeIconv(*res); };
  CharsetConverter conv = MakeIconvConverter(*res);

  text* abc = conv.proc(conv.context, "abc", 3);
  ASSERT_NE(abc, nullptr);
  absl::Cleanup abc_cleanup = [abc] { pfree(abc); };
  EXPECT_EQ(StringViewFromText(abc), "abc");

  // абв in utf-8
  text* russian = conv.proc(conv.context, "\xd0\xb0\xd0\xb1\xd0\xb2", 6);
  ASSERT_NE(russian, nullptr);
  absl::Cleanup russian_cleanup = [russian] { pfree(russian); };
  // абв in cp1251
  EXPECT_EQ(StringViewFromText(russian), "\xe0\xe1\xe2");

  // xüåøx in utf-8
  text* unrepresentable = conv.proc(conv.context, "x\xc3\xbc\xc3\xa5\xc3\xb8x", 8);
  ASSERT_NE(unrepresentable, nullptr);
  absl::Cleanup unrepresentable_cleanup = [unrepresentable] { pfree(unrepresentable); };
  auto unrepresentable_string = StringViewFromText(unrepresentable);
  // iconv may behave differently on different platforms, sometimes substituting
  // unrepresentable symbols with close approximation, otherwise IconvConverter
  // would replace the symbols with '?'. Check that representable symbols are
  // preserved and each of the other symbols are replaced with at least one
  // substitution character
  EXPECT_EQ(unrepresentable_string.front(), 'x');
  EXPECT_EQ(unrepresentable_string.back(), 'x');
  EXPECT_GE(unrepresentable_string.size(), 5);
}

}  // namespace
}  // namespace tea
