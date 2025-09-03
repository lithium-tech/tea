#include "tea/smoke_test/perf_test/json_table.h"

#include <gen/src/list.h>

#include <string_view>
#include <vector>

#include "gen/src/generators.h"
#include "gen/src/program.h"
#include "gen/src/table.h"

namespace gen {

std::shared_ptr<arrow::Schema> JsonTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kJsonColumn), arrow::utf8()));
  return std::make_shared<arrow::Schema>(fields);
}

Program MakeJsonProgram(RandomDevice& random_device, int32_t non_escaped_patterns, int32_t escaped_patterns) {
  Program program;

  std::string x = "{\"key\": \"";
  for (int32_t i = 0; i < non_escaped_patterns; ++i) {
    x += "abcdef";
  }
  for (int32_t i = 0; i < escaped_patterns; ++i) {
    x += "\\u0422";
  }
  x += "\"}";

  gen::List list("list", std::vector<std::string>{x});
  program.AddAssign(Assignment(JsonTable::kJsonColumn, std::make_shared<StringFromListGenerator>(list, random_device)));

  return program;
}

}  // namespace gen
