#include "tea/filter/gp/validate.h"

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "catalog/pg_type.h"
}

#ifdef Abs
#undef Abs
#endif

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "iceberg/filter/representation/column_extractor.h"
#include "iceberg/filter/representation/visitor.h"
#include "iceberg/filter/row_filter/registry.h"
#include "iceberg/filter/row_filter/row_filter.h"

#include "tea/filter/gp/datetime.h"

namespace tea {

namespace {

arrow::Result<std::shared_ptr<arrow::Field>> PGToArrowField(Form_pg_attribute attr) {
  std::string column_name(attr->attname.data);
  switch (attr->atttypid) {
    case INT2OID:
      return std::make_shared<arrow::Field>(column_name, arrow::int16());
    case INT4OID:
      return std::make_shared<arrow::Field>(column_name, arrow::int32());
    case INT8OID:
      return std::make_shared<arrow::Field>(column_name, arrow::int64());
    case FLOAT4OID:
      return std::make_shared<arrow::Field>(column_name, arrow::float32());
    case FLOAT8OID:
      return std::make_shared<arrow::Field>(column_name, arrow::float64());
    case TEXTOID:
      return std::make_shared<arrow::Field>(column_name, arrow::utf8());
    case TIMESTAMPOID:
      return std::make_shared<arrow::Field>(column_name, arrow::timestamp(arrow::TimeUnit::MICRO));
    case TIMESTAMPTZOID:
      return std::make_shared<arrow::Field>(column_name, arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
    case DATEOID:
      return std::make_shared<arrow::Field>(column_name, arrow::date32());
    case TIMEOID:
      return std::make_shared<arrow::Field>(column_name, arrow::time64(arrow::TimeUnit::MICRO));
    case BOOLOID:
      return std::make_shared<arrow::Field>(column_name, arrow::boolean());
    case NUMERICOID: {
      const auto precision = ((attr->atttypmod - 4) >> 16) & 65535;
      const auto scale = (attr->atttypmod - 4) & 65535;
      return std::make_shared<arrow::Field>(column_name, arrow::decimal128(precision, scale));
    }
  }

  return arrow::Status::NotImplemented("Conversion from ", attr->atttypid, " is not supported");
}

std::string ToLower(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), [](char ch) { return std::tolower(ch); });
  return str;
}

}  // namespace

arrow::Status ValidateRowFilterClause(iceberg::filter::NodePtr expr, TupleDesc tuple_desc) {
  std::map<std::string, int> name_to_index;
  for (int i = 0; i < tuple_desc->natts; ++i) {
    std::string column_name = tuple_desc->attrs[i]->attname.data;
    column_name = ToLower(std::move(column_name));
    name_to_index.emplace(std::move(column_name), i);
  }

  iceberg::filter::SubtreeVisitor<iceberg::filter::ColumnExtractorVisitor> column_extractor;
  column_extractor.Visit(expr);
  auto used_columns = column_extractor.GetResult();

  auto maybe_schema = [&]() -> arrow::Result<std::shared_ptr<arrow::Schema>> {
    arrow::FieldVector vec;
    for (auto used_column : used_columns) {
      used_column = ToLower(std::move(used_column));

      if (auto it = name_to_index.find(used_column); it != name_to_index.end()) {
        int i = it->second;
        ARROW_ASSIGN_OR_RAISE(auto field, PGToArrowField(tuple_desc->attrs[i]));
        vec.emplace_back(field);
      } else {
        return arrow::Status::ExecutionError("Internal error in tea: unexpected column name ", used_column);
      }
    }

    return std::make_shared<arrow::Schema>(std::move(vec));
  }();

  if (maybe_schema.ok()) {
    auto schema = maybe_schema.ValueUnsafe();

    iceberg::filter::RowFilter row_filter(expr);
    return row_filter.BuildFilter(std::make_shared<iceberg::filter::TrivialArrowFieldResolver>(schema),
                                  std::make_shared<iceberg::filter::GandivaFunctionRegistry>([]() {
                                    bool overflow;
                                    return Timestamp2Timestamptz(0, &overflow);
                                  }()),
                                  schema);

  } else {
    return arrow::Status::ExecutionError("Failed to build schema. ", maybe_schema.status().message());
  }
}

}  // namespace tea
