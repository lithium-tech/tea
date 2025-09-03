#pragma once

#include <iconv.h>

#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "iceberg/nested_field.h"

#include "tea/table/gp_fwd.h"

namespace tea {

struct ReaderColumn {
  // ---------------------
  // Filled at query start

  // Column name in Greenplum relation and Iceberg table
  std::string gp_name;
  // Column index in Greenplum relation
  int gp_index;
  // Greenplum type of the column
  Oid gp_type;
  // Greenplum type mode of the column
  int gp_type_mode;

  bool remote_only;
};

using CharsetConverterProc = varlena* (*)(void* context, const char* str, size_t len);

struct CharsetConverter {
  CharsetConverterProc proc;
  void* context;
};

CharsetConverter MakeIdentityConverter();
CharsetConverter MakePgConverter(FmgrInfo* converter_proc);
CharsetConverter MakeIconvConverter(iconv_t instance);
arrow::Result<iconv_t> InitializeIconv(int db_encoding);
arrow::Status FinalizeIconv(iconv_t enc);

GpDatum GetDatumFromArrow(const Oid gp_type, const std::shared_ptr<arrow::Array>& array, const int64_t row,
                          CharsetConverter converter, bool substitute_unicode);

bool MatchArrowColumn(const std::shared_ptr<arrow::DataType>& type, const Oid gp_type, const int gp_type_mode);

using FieldId = int32_t;
std::optional<FieldId> MatchIcebergColumn(const iceberg::types::NestedField& field, const Oid gp_type);

}  // namespace tea
