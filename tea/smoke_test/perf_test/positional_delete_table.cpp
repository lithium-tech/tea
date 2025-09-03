#include "tea/smoke_test/perf_test/positional_delete_table.h"

#include <string_view>

#include "gen/src/generators.h"
#include "gen/src/program.h"

namespace gen {

std::shared_ptr<arrow::Schema> PositionalDeleteTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kInt4Columns), arrow::int32()));
  return std::make_shared<arrow::Schema>(fields);
}

Program MakePositionalDeleteProgram() {
  Program program;

  program.AddAssign(
      Assignment(PositionalDeleteTable::kInt4Columns, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>(1)));

  return program;
}

}  // namespace gen
