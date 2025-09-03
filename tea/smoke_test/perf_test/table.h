#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "arrow/type.h"
#include "gen/src/program.h"
#include "gen/src/table.h"

namespace gen {

struct AllTypesTable : public Table {
  static constexpr std::string_view kInt2Column = "c_int2";
  static constexpr std::string_view kInt4Column = "c_int4";
  static constexpr std::string_view kInt8Column = "c_int8";
  static constexpr std::string_view kStringColumn = "c_string";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "all_types_table"; }
};

Program MakeAllTypesProgram(RandomDevice& random_device);

}  // namespace gen
