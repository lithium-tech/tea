#pragma once

#include <memory>
#include <string>
#include <string_view>

#include "arrow/type.h"
#include "gen/src/program.h"
#include "gen/src/table.h"

namespace gen {

struct PositionalDeleteTable : public Table {
  static constexpr std::string_view kInt4Columns = "c_int4";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "positional_delete_table"; }
};

Program MakePositionalDeleteProgram();

}  // namespace gen
