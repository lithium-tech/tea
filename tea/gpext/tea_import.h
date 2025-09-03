#pragma once

#include "tea/gpext/tea_reader.h"
#include "tea/gpext/tea_tuple.h"

/* Distributed transaction id (10 digit timestamp, a dash, a 10 digit distributed transaction id, and NUL)
 * + GP session id + GP command count + 2 delimiters */
#define SESSION_ID_LEN (TMGIDSIZE + 11 + 10 + 2)

void GetScanSessionId(char* buf, int size);

typedef struct {
  TeaContextPtr tea_ctx;
  ReaderOptions options;
  /* tupdesc->natts sized, valid up to ncolumns */
  int* columns;
  int ncolumns;
  char session_id[SESSION_ID_LEN];
  HeapFillTupleInfo heap_form_tuple_info;
  /* tupdesc->natts sized */
  Datum* values;
  /* tupdesc->natts sized */
  bool* nulls;
} Import;

static inline void UpdateExtStats(Import* import, int64_t start, int64_t end) {
  import->tea_ctx->ext_stats.heap_form_tuple_ticks += end - start;
}
