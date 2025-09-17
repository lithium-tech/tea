#include "tea/smoke_test/filter_tests/filter_test_base.h"

namespace tea {
namespace {

class FilterTestLikeOperator : public FilterTestBase {};

TEST_F(FilterTestLikeOperator, StartsWith) {
  std::string str_aac = "aac";
  std::string str_abc = "abc";
  std::string str_abcd = "abcd";
  std::string str_abd = "abd";
  std::string str_ac = "ac";
  auto column1 =
      MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_aac, &str_abc, &str_abcd, &str_abd, &str_ac});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 like 'abc%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"starts-with\",\"term\":\"col1\","
                                            "\"value\":\"abc\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abc"}, {"abcd"}}))
                        .SetGandivaFilters({"bool like((string) col1, (const string) 'abc%', "
                                            "(const string) '\\')"}));
  ProcessWithFilter("col1", "col1 like '%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"starts-with\",\"term\":\"col1\",\"value\":\"\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"aac"}, {"abc"}, {"abcd"}, {"abd"}, {"ac"}}))
                        .SetGandivaFilters({"bool like((string) col1, (const string) '%', "
                                            "(const string) '\\')"}));
  ProcessWithFilter("col1", "col1 not like 'abc%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"not-starts-with\",\"term\":\"col1\","
                                            "\"value\":\"abc\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"aac"}, {"abd"}, {"ac"}}))
                        .SetGandivaFilters({"bool not(bool like((string) col1, (const "
                                            "string) 'abc%', (const string) '\\'))"}));
}

TEST_F(FilterTestLikeOperator, EmptyLine) {
  std::string str1 = "\n\n\ni am abc_def_ghi.jkl by the way\n\n\n\n";
  std::string str2 = "\t\t\ti am abc_def_ghi.jkl by the way\t\t\t\t";
  std::string str3 = "i am abc_def_ghi.jkl by the way";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2, &str3});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 like '%abc_def_ghi.jkl%'",
                    ExpectedValues()
                        .SetSelectResult(pq::ScanResult({"col1"}, {{str1}, {str2}, {str3}}))
                        .SetGandivaFilters({"bool like((string) col1, (const string) '%abc_def_ghi.jkl%', "
                                            "(const string) '\\')"}));
}

TEST_F(FilterTestLikeOperator, NoFilter) {
  std::string str1 = "";
  std::string str2 = "ab";
  std::string str3 = "abc";
  std::string str4 = "a%bc";
  std::string str5 = "azzbc";
  std::string str6 = "azbc";
  std::string str7 = "azbcqqq";
  auto column1 =
      MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2, &str3, &str4, &str5, &str6, &str7});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 like ''",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{""}}))
                        .SetGandivaFilters({"bool like((string) col1, (const string) '', "
                                            "(const string) '\\')"}));
  ProcessWithFilter("col1", "col1 like 'abc'",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abc"}}))
                        .SetGandivaFilters({"bool like((string) col1, (const string) 'abc', "
                                            "(const string) '\\')"}));
  ProcessWithFilter("col1", "col1 like 'a%bc'",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"abc"}, {"a%bc"}, {"azzbc"}, {"azbc"}}))
                        .SetGandivaFilters({"bool like((string) col1, (const string) 'a%bc', "
                                            "(const string) '\\')"}));
  ProcessWithFilter("col1", "col1 like 'a_bc%'",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"a%bc"}, {"azbc"}, {"azbcqqq"}}))
                        .SetGandivaFilters({"bool like((string) col1, (const string) "
                                            "'a_bc%', (const string) '\\')"}));
}

TEST_F(FilterTestLikeOperator, EscapeCharacters) {
  std::string str1 = "a\\bcqwe";
  std::string str2 = "a\\bc";
  std::string str3 = "a_bcw";
  std::string str4 = "aqbcw";
  std::string str5 = "a%bcq";
  std::string str6 = "a%24bcq";
  std::string str7 = "%_%\\zz";
  std::string str8 = "%_%\\z";
  std::string str9 = "\\_q";
  std::string str10 = "\\%%";
  std::string str11 = "\\%";
  std::string str12 = "\\%q";
  auto column1 = MakeStringColumn("col1", 1,
                                  std::vector<std::string*>{nullptr, &str1, &str2, &str3, &str4, &str5, &str6, &str7,
                                                            &str8, &str9, &str10, &str11, &str12});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 like 'a\\\\bc%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"starts-with\",\"term\":\"col1\","
                                            "\"value\":\"a\\\\bc\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"a\\bcqwe"}, {"a\\bc"}}))
                        .SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 like 'a\\_bc%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"starts-with\",\"term\":\"col1\","
                                            "\"value\":\"a_bc\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"a_bcw"}}))
                        .SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 like 'a\\%bc%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"starts-with\",\"term\":\"col1\","
                                            "\"value\":\"a%bc\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"a%bcq"}}))
                        .SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 like '\\%\\_\\%\\\\%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"starts-with\",\"term\":\"col1\","
                                            "\"value\":\"%_%\\\\\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"%_%\\zz"}, {"%_%\\z"}}))
                        .SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 like '\\\\%'",
                    ExpectedValues()
                        .SetIcebergFilters({"{\"type\":\"starts-with\",\"term\":\"col1\","
                                            "\"value\":\"\\\\\"}"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"\\_q"}, {"\\%%"}, {"\\%"}, {"\\%q"}}))
                        .SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 like '\\\\\\%'",
                    ExpectedValues()
                        .SetIcebergFilters({""})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"\\%"}}))
                        .SetGandivaFilters({""}));
}

class FilterTestILikeOperator : public FilterTestBase {};

TEST_F(FilterTestILikeOperator, Simple) {
  std::string str_aac = "Aac";
  std::string str_abc = "aBc";
  std::string str_abcd = "abCd";
  std::string str_abd = "AbD";
  std::string str_ac = "AC";
  auto column1 =
      MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str_aac, &str_abc, &str_abcd, &str_abd, &str_ac});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "col1 ilike 'abc%'",
                    ExpectedValues()
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"aBc"}, {"abCd"}}))
                        .SetGandivaFilters({"bool ilike((string) col1, (const string) 'abc%')"}));
  ProcessWithFilter("col1", "col1 ilike '%'",
                    ExpectedValues()
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Aac"}, {"aBc"}, {"abCd"}, {"AbD"}, {"AC"}}))
                        .SetGandivaFilters({"bool ilike((string) col1, (const string) '%')"}));
  ProcessWithFilter("col1", "col1 not ilike 'abc%'",
                    ExpectedValues()
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Aac"}, {"AbD"}, {"AC"}}))
                        .SetGandivaFilters({"bool not(bool ilike((string) col1, (const "
                                            "string) 'abc%'))"}));
}

TEST_F(FilterTestILikeOperator, EscapeCharacters) {
  std::string str1 = "A\\bcqwe";
  std::string str2 = "a\\Bc";
  std::string str3 = "a_Bcw";
  std::string str4 = "AqbcW";
  std::string str5 = "A%bcQ";
  std::string str6 = "a%24BcQ";
  std::string str7 = "%_%\\zZ";
  std::string str8 = "%_%\\z";
  std::string str9 = "\\_q";
  std::string str10 = "\\%%";
  std::string str11 = "\\%";
  std::string str12 = "\\%q";
  auto column1 = MakeStringColumn("col1", 1,
                                  std::vector<std::string*>{nullptr, &str1, &str2, &str3, &str4, &str5, &str6, &str7,
                                                            &str8, &str9, &str10, &str11, &str12});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter(
      "col1", "col1 ilike 'a\\\\bc%'",
      ExpectedValues().SetSelectResult(pq::ScanResult({"col1"}, {{"A\\bcqwe"}, {"a\\Bc"}})).SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 ilike 'a\\_bc%'",
                    ExpectedValues().SetSelectResult(pq::ScanResult({"col1"}, {{"a_Bcw"}})).SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 ilike 'a\\%bc%'",
                    ExpectedValues().SetSelectResult(pq::ScanResult({"col1"}, {{"A%bcQ"}})).SetGandivaFilters({""}));
  ProcessWithFilter(
      "col1", "col1 ilike '\\%\\_\\%\\\\%'",
      ExpectedValues().SetSelectResult(pq::ScanResult({"col1"}, {{"%_%\\zZ"}, {"%_%\\z"}})).SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 ilike '\\\\%'",
                    ExpectedValues()
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"\\_q"}, {"\\%%"}, {"\\%"}, {"\\%q"}}))
                        .SetGandivaFilters({""}));
  ProcessWithFilter("col1", "col1 ilike '\\\\\\%'",
                    ExpectedValues().SetSelectResult(pq::ScanResult({"col1"}, {{"\\%"}})).SetGandivaFilters({""}));
}

class FilterTestString : public FilterTestBase {};

TEST_F(FilterTestString, Concatenation) {
  std::string str1 = "Get";
  std::string str2 = "Post";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str1, &str2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "(col1 || 'greSQL') = 'PostgreSQL'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string concatOperator((string) col1, (const "
                                            "string) 'greSQL'), (const string) 'PostgreSQL')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Post"}})));
  ProcessWithFilter("col1", "('gre' || col1 || 'SQL') = 'grePostSQL'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string concatOperator(string concatOperator((const "
                                            "string) 'gre', (string) col1), (const string) "
                                            "'SQL'), (const string) 'grePostSQL')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Post"}})));
  ProcessWithFilter("col1", "(col1 || 42 || 'greSQL') = 'Post42greSQL'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string concatOperator(string concatOperator((string) "
                                            "col1, (const string) '42'), (const string) "
                                            "'greSQL'), (const string) 'Post42greSQL')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Post"}})));
}

TEST_F(FilterTestString, ConcatenationWithNull) {
  std::string str_a = "a";
  std::string str_b = "b";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str_a, nullptr, &str_a, nullptr});
  auto column2 = MakeStringColumn("col2", 2, std::vector<std::string*>{&str_b, &str_a, nullptr, nullptr});
  auto column3 = MakeInt32Column("col3", 3, OptionalVector<int32_t>{1, 2, 3, 4});

  PrepareData({column1, column2, column3},
              {GreenplumColumnInfo{.name = "col1", .type = "text"}, GreenplumColumnInfo{.name = "col2", .type = "text"},
               GreenplumColumnInfo{.name = "col3", .type = "int4"}});
  ProcessWithFilter("col3", "(col1 || col2) is null",
                    ExpectedValues()
                        .SetGandivaFilters({"bool isnull(string concatOperator((string) col1, (string) col2))"})
                        .SetSelectResult(pq::ScanResult({"col3"}, {{"2"}, {"3"}, {"4"}})));
}

TEST_F(FilterTestString, Length) {
  std::string str1 = "Get";
  std::string str2 = "Post";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str1, &str2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "char_length(col1) = 4",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(int32 char_length((string) col1), (const int32) 4)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Post"}})));

  ProcessWithFilter("col1", "length(col1) = 4",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(int32 char_length((string) col1), (const int32) 4)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Post"}})));
}

// TODO(gmusya): check correctness for cp-1251 and utf-8
TEST_F(FilterTestString, Lower) {
  std::string str1 = "Get";
  std::string str2 = "Пост";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str1, &str2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "lower(col1) = 'get'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string lower((string) col1), (const string) 'get')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Get"}})));
}

TEST_F(FilterTestString, Upper) {
  std::string str1 = "Get";
  std::string str2 = "Пост";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str1, &str2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "upper(col1) = 'GET'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string upper((string) col1), (const string) 'GET')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"Get"}})));
}

TEST_F(FilterTestString, Trim) {
  std::string str1 = "xxTomxxx";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str1});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "trim(both 'x' from col1) = 'Tom'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string btrim((string) col1, (const "
                                            "string) 'x'), (const string) 'Tom')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"xxTomxxx"}})));
  ProcessWithFilter("col1", "trim(leading 'x' from col1) = 'Tomxxx'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string ltrim((string) col1, (const "
                                            "string) 'x'), (const string) 'Tomxxx')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"xxTomxxx"}})));
  ProcessWithFilter("col1", "trim(trailing 'x' from col1) = 'xxTom'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string rtrim((string) col1, (const "
                                            "string) 'x'), (const string) 'xxTom')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"xxTomxxx"}})));
}

TEST_F(FilterTestString, Substring) {
  std::string str1 = "12-34-56-78";
  std::string str2 = "12-99-56-78";
  std::string str3 = "12-32-56-78";
  std::string str4 = "12-34-00-00";
  std::string str5 = "12-36-56-78";
  std::string str6 = "12-8";
  auto column1 =
      MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2, &str3, &str4, &str5, &str6});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "substring(col1 from 4 for 2) = '34'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string substring((string) col1, int64 "
                                            "castBIGINT((const int32) 4), int64 castBIGINT((const int32) "
                                            "2)), (const string) '34')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"12-34-56-78"}, {"12-34-00-00"}})));
  ProcessWithFilter("col1", "substring(col1 from 6) = '-56-78'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string substring((string) col1, int64 "
                                            "castBIGINT((const int32) 6)), (const string) '-56-78')"})
                        .SetSelectResult(pq::ScanResult(
                            {"col1"}, {{"12-34-56-78"}, {"12-99-56-78"}, {"12-32-56-78"}, {"12-36-56-78"}})));
  ProcessWithFilter("col1", "substr(col1, 4, 2) = '34'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string substring((string) col1, int64 "
                                            "castBIGINT((const int32) 4), int64 castBIGINT((const int32) "
                                            "2)), (const string) '34')"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"12-34-56-78"}, {"12-34-00-00"}})));
  ProcessWithFilter("col1", "substr(col1, 6) = '-56-78'",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(string substring((string) col1, int64 "
                                            "castBIGINT((const int32) 6)), (const string) '-56-78')"})
                        .SetSelectResult(pq::ScanResult(
                            {"col1"}, {{"12-34-56-78"}, {"12-99-56-78"}, {"12-32-56-78"}, {"12-36-56-78"}})));
}

TEST_F(FilterTestString, Position) {
  std::string str1 = "om";
  std::string str2 = "qwe";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{&str1, &str2});
  PrepareData({column1}, {GreenplumColumnInfo{.name = "col1", .type = "text"}});
  ProcessWithFilter("col1", "position(col1 in 'Thomas') = 3",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(int32 locate((string) col1, (const "
                                            "string) 'Thomas'), (const int32) 3)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"om"}})));
  ProcessWithFilter("col1", "position(col1 in 'Thomas') = 0",
                    ExpectedValues()
                        .SetGandivaFilters({"bool equal(int32 locate((string) col1, (const "
                                            "string) 'Thomas'), (const int32) 0)"})
                        .SetSelectResult(pq::ScanResult({"col1"}, {{"qwe"}})));
}

}  // namespace
}  // namespace tea
