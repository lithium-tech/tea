#include "tea/smoke_test/perf_test/table.h"

#include <string_view>

#include "gen/src/generators.h"
#include "gen/src/program.h"
#include "gen/src/table.h"

namespace gen {

std::shared_ptr<arrow::Schema> AllTypesTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kInt2Column), arrow::int16()));
  fields.emplace_back(arrow::field(std::string(kInt4Column), arrow::int32()));
  fields.emplace_back(arrow::field(std::string(kInt8Column), arrow::int64()));
  fields.emplace_back(arrow::field(std::string(kStringColumn), arrow::utf8()));
  return std::make_shared<arrow::Schema>(fields);
}

Program MakeAllTypesProgram(RandomDevice& random_device) {
  Program program;

  program.AddAssign(
      Assignment(AllTypesTable::kInt2Column, std::make_shared<UniqueIntegerGenerator<arrow::Int16Type>>(1)));
  program.AddAssign(
      Assignment(AllTypesTable::kInt4Column, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>(1)));
  program.AddAssign(
      Assignment(AllTypesTable::kInt8Column, std::make_shared<UniqueIntegerGenerator<arrow::Int64Type>>(1)));
  program.AddAssign(Assignment(AllTypesTable::kStringColumn,
                               std::make_shared<StringFromCharsetGenerator>(1, 5, "abcde123", random_device)));

  return program;
}

}  // namespace gen
