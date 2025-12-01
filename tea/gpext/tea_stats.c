#include "postgres.h"

#include "catalog/pg_statistic.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "tea/gpext/tea_import.h"
#include "tea/gpext/tea_reader.h"

PG_FUNCTION_INFO_V1(tea_get_stats_from_iceberg);

typedef struct GetIcebergStatsCallContext {
  Oid table_oid;
  Relation rel;
  TupleDesc table_tupdesc;
  TeaContextPtr tea_ctx;
  const char* table_location;
  int last_processed_attno;
} GetIcebergStatsCallContext;

void GetStatsPrepare(FunctionCallInfo fcinfo, FuncCallContext* funcctx) {
  if (get_call_result_type(fcinfo, NULL, &funcctx->tuple_desc) != TYPEFUNC_COMPOSITE) {
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("function returning record called in context "
                                                                   "that cannot accept type record")));
  }

  funcctx->user_fctx = palloc0(sizeof(GetIcebergStatsCallContext));
  GetIcebergStatsCallContext* user_ctx = (GetIcebergStatsCallContext*)funcctx->user_fctx;
  user_ctx->table_oid = PG_GETARG_OID(0);
  user_ctx->rel = heap_open(user_ctx->table_oid, AccessShareLock);
  user_ctx->table_tupdesc = RelationGetDescr(user_ctx->rel);
  funcctx->attinmeta = TupleDescGetAttInMetadata(funcctx->tuple_desc);
  user_ctx->table_location = text_to_cstring(PG_GETARG_TEXT_P(1));
  if (user_ctx->table_location == NULL) {
    elog(ERROR, "Tea error: cannot tea options");
  }
  user_ctx->tea_ctx = TeaContextCreate(user_ctx->table_location);
}

ColumnStats GetColumnStats(FunctionCallInfo fcinfo, FuncCallContext* funcctx, int attrno_to_process) {
  ColumnStats result;
  GetIcebergStatsCallContext* user_ctx = (GetIcebergStatsCallContext*)funcctx->user_fctx;
  char session_id[SESSION_ID_LEN];
  GetScanSessionId(session_id, SESSION_ID_LEN);
  TeaContextGetIcebergColumnStats(user_ctx->table_location, session_id,
                                  user_ctx->table_tupdesc->attrs[attrno_to_process - 1]->attname.data, &result);
  return result;
}

#define STARELID_NUM 0
#define STAATTNUM_NUM 1
#define STANULLFRAC_NUM 2
#define STAWIDTH_NUM 3
#define STADISTINCT_NUM 4

void ColumnStatsToDatumArray(ColumnStats* stats, Datum* values, bool* isnull, Oid table_oid, int attno) {
  values[STARELID_NUM] = ObjectIdGetDatum(table_oid);
  values[STAATTNUM_NUM] = Int16GetDatum(attno);
  if (stats->null_count != -1 && stats->not_null_count != -1 && stats->null_count + stats->not_null_count != 0) {
    float null_frac = (float)stats->null_count / ((float)stats->null_count + (float)stats->not_null_count);
    values[STANULLFRAC_NUM] = Float4GetDatum(null_frac);
  } else {
    isnull[STANULLFRAC_NUM] = true;
  }
  if (stats->not_null_count > 0 && stats->total_compressed_size != -1) {
    values[STAWIDTH_NUM] = Int32GetDatum(stats->total_compressed_size / stats->not_null_count);
  } else {
    isnull[STAWIDTH_NUM] = true;
  }
  if (stats->distinct_count != -1 && stats->not_null_count > 0) {
    values[STADISTINCT_NUM] = Float4GetDatum(stats->distinct_count);

    // float frac = (float)stats.distinct_count / (float)stats.not_null_count;
    // values[Anum_pg_statistic_stadistinct - 1] = Float4GetDatum(-frac);  // catalog/pg_statistics.h
  } else {
    isnull[STADISTINCT_NUM] = true;
  }
}

// tea_get_stats_from_iceberg(oid, location)
Datum tea_get_stats_from_iceberg(PG_FUNCTION_ARGS) {
  FuncCallContext* funcctx;

  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    GetStatsPrepare(fcinfo, funcctx);
    MemoryContextSwitchTo(oldcontext);
  }

  funcctx = SRF_PERCALL_SETUP();

  GetIcebergStatsCallContext* user_ctx = (GetIcebergStatsCallContext*)funcctx->user_fctx;
  int attrno_to_process = funcctx->call_cntr + 1;
  if (attrno_to_process <= user_ctx->table_tupdesc->natts) {
    ColumnStats stats = GetColumnStats(fcinfo, funcctx, attrno_to_process);

    Datum values[Anum_pg_statistic_stadistinct] = {};
    bool isnull[Anum_pg_statistic_stadistinct] = {};

    ColumnStatsToDatumArray(&stats, values, isnull, user_ctx->table_oid, attrno_to_process);

    HeapTuple tuple;
    tuple = heap_form_tuple(funcctx->attinmeta->tupdesc, values, isnull);

    Datum result;
    result = HeapTupleGetDatum(tuple);

    SRF_RETURN_NEXT(funcctx, result);
  } else {
    heap_close(user_ctx->rel, AccessShareLock);
    TeaContextDestroy(user_ctx->tea_ctx);
    pfree(funcctx->user_fctx);
    SRF_RETURN_DONE(funcctx);
  }
}
