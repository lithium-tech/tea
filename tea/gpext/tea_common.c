#include "tea/gpext/tea_common.h"

ReaderScanProjection MakeScanProjection(TupleDesc tupdesc, int ncolumns, int *column_attnums, bool *is_remote_only) {
  ReaderScanProjection projection;
  projection.ncolumns = ncolumns;
  projection.columns = (ReaderScanColumn *)palloc0(sizeof(ReaderScanColumn) * ncolumns);
  for (int i = 0; i < ncolumns; ++i) {
    ReaderScanColumn *column = projection.columns + i;
    Form_pg_attribute attr = tupdesc->attrs[column_attnums[i]];
    column->index = column_attnums[i];
    column->name = attr->attname.data;
    column->type = attr->atttypid;
    column->type_mode = attr->atttypmod;
    column->remote_only = is_remote_only ? is_remote_only[i] : false;
  }

  return projection;
}
