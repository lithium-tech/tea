#include "tea/smoke_test/filter_tests/filter_test_base.h"

#include <set>
#include <vector>

#include "iceberg/test_utils/column.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/mock_teapot.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"

namespace tea {
namespace {

std::string ToString(const std::set<std::string>& s) {
  std::stringstream os;
  os << "{ ";
  bool is_first = true;
  for (auto& now : s) {
    if (!is_first) {
      os << ", ";
    } else {
      is_first = false;
    }
    os << "\"" << now << "\"";
  }
  if (!is_first) {
    os << " ";
  }
  os << "}";
  return os.str();
}

}  // namespace

void FilterTestBase::PrepareData(const std::vector<ParquetColumn>& columns,
                                 const std::vector<GreenplumColumnInfo>& gp_columns) {
  ASSIGN_OR_FAIL(auto data_path, state_->WriteFile(columns));
  ASSERT_OK(state_->AddDataFiles({data_path}));

  if (drop_table_defer_.has_value()) {
    drop_table_defer_.reset();
  }

  ASSIGN_OR_FAIL(auto defer, state_->CreateTable(gp_columns));
  drop_table_defer_.emplace(std::move(defer));
}

void FilterTestBase::ProcessWithFilter(const std::vector<std::string>& column_names, const std::string& condition,
                                       const ExpectedValues& expected) {
  auto maybe_result = pq::TableScanQuery(kDefaultTableName, column_names).SetWhere(condition).Run(*conn_);
  if (expected.is_error) {
    ASSERT_FALSE(maybe_result.ok());
  } else {
    ASSERT_TRUE(maybe_result.ok()) << maybe_result.status().message();
  }
  if (expected.iceberg_filters.has_value() && Environment::GetMetadataType() == MetadataType::kTeapot) {
    const auto& expected_iceberg_filters = *expected.iceberg_filters;
    auto actual_iceberg_filter = teapot_->GetLastRequest().iceberg_expression_json();
    EXPECT_TRUE(expected_iceberg_filters.contains(actual_iceberg_filter))
        << "condition: " << condition << "\n"
        << "expected: " << ToString(expected_iceberg_filters) << "\n"
        << "found: \"" << actual_iceberg_filter << "\"";
  }
  if (expected.gandiva_filters) {
    const auto& expected_gandiva_filters = *expected.gandiva_filters;
    auto actual_gandiva_filter = stats_state_->GetLastGandivaFilter();
    EXPECT_TRUE(expected_gandiva_filters.contains(actual_gandiva_filter))
        << "condition: " << condition << "\n"
        << "expected: " << ToString(expected_gandiva_filters) << "\n"
        << "found: \"" << actual_gandiva_filter << "\"";
  }
  if (expected.select_result) {
    auto result = std::move(maybe_result.ValueUnsafe());
    const auto& expected_select_result = *expected.select_result;
    EXPECT_EQ(result, expected_select_result) << "condition: " << condition;
  }
  stats_state_->ClearFilters();
}

}  // namespace tea
