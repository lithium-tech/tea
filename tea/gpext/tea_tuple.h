#pragma once

#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"

typedef struct HeapFillTupleInfo {
  int32_t *retrieved_columns_indices;
  uint32_t retrieved_columns_indices_length;
} HeapFillTupleInfo;

HeapTuple TeaHeapFormTuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull,
                           const HeapFillTupleInfo *heap_fill_tuple_info);

void InitHeapFormTupleInfo(HeapFillTupleInfo *result, int *columns, int ncolumns);
