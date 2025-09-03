#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "arrow/type.h"
#include "gen/src/program.h"
#include "gen/src/table.h"

namespace gen {

struct JsonTable : public Table {
  static constexpr std::string_view kJsonColumn = "c_json";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "json_table"; }
};

Program MakeJsonProgram(RandomDevice& random_device, int32_t value_non_escaped_chars, int32_t value_escaped_chars);

}  // namespace gen
