#pragma once

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/test_base.h"

namespace tea {

struct ExpectedValues {
  bool is_error = false;
  std::optional<std::set<std::string>> iceberg_filters;
  std::optional<std::set<std::string>> gandiva_filters;
  std::optional<pq::ScanResult> select_result;

  ExpectedValues& SetIsError(bool flag) {
    is_error = flag;
    return *this;
  }

  ExpectedValues& SetIcebergFilters(std::set<std::string> filters) {
    iceberg_filters = std::move(filters);
    return *this;
  }

  ExpectedValues& SetGandivaFilters(std::set<std::string> filters) {
    gandiva_filters = std::move(filters);
    return *this;
  }

  ExpectedValues& SetSelectResult(pq::ScanResult result) {
    select_result = std::move(result);
    return *this;
  }
};

// before running tests
// gpinitsystem for 1 segment and install tea
class FilterTestBase : public TeaTest {
 protected:
  void PrepareData(const std::vector<ParquetColumn>& columns, const std::vector<GreenplumColumnInfo>& gp_columns);

  void ProcessWithFilter(const std::vector<std::string>& column_names, const std::string& condition,
                         const ExpectedValues& expected);

  inline void ProcessWithFilter(const std::string& column_name, const std::string& condition,
                                const ExpectedValues& expected) {
    ProcessWithFilter(std::vector<std::string>({column_name}), condition, expected);
  }

  std::optional<pq::DropTableDefer> drop_table_defer_;
};

}  // namespace tea
