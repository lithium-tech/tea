#include <assert.h>
#include <inttypes.h>
#include <stdint.h>

#include "postgres.h"

#include "access/extprotocol.h"
#include "access/fileam.h"
#include "access/formatter.h"
#include "access/htup.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "storage/ipc.h"
#include "utils/builtins.h"

#include "tea/filter/gp/convert.h"
#include "tea/gpext/tea_column.h"
#include "tea/gpext/tea_common.h"
#include "tea/gpext/tea_exttable_proj.h"
#include "tea/gpext/tea_import.h"
#include "tea/gpext/tea_reader.h"
#include "tea/gpext/tea_tuple.h"
#include "tea/util/measure_c.h"

#define TEA_SCHEMA "tea://"

PG_FUNCTION_INFO_V1(teaprotocol_validate_urls);
Datum teaprotocol_validate_urls(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(teaprotocol_import);
Datum teaprotocol_import(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(teaprotocol_export);
Datum teaprotocol_export(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(teaformat_import);
Datum teaformat_import(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(teaformat_export);
Datum teaformat_export(PG_FUNCTION_ARGS);

static Datum FetchNextTuple(PG_FUNCTION_ARGS);

static bool HasPrefix(char *s, char *prefix) { return strncmp(s, prefix, strlen(prefix)) == 0; }

Datum teaprotocol_validate_urls(PG_FUNCTION_ARGS) {
  if (!CALLED_AS_EXTPROTOCOL_VALIDATOR(fcinfo)) {
    elog(ERROR,
         "Tea error: expected call through external protocol validation "
         "protocol");
  }

  ValidatorDirection direction = EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo);
  if (direction != EXT_VALIDATE_READ) {
    ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("Only readable tables are supported")));
  }

  int nurls = EXTPROTOCOL_VALIDATOR_GET_NUM_URLS(fcinfo);
  if (nurls != 1) {
    ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("Unexpected number of urls (1 expected): %d", nurls)));
  }

  char *url = EXTPROTOCOL_VALIDATOR_GET_NTH_URL(fcinfo, 1);
  if (!HasPrefix(url, TEA_SCHEMA)) {
    ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("Expected " TEA_SCHEMA " protocol")));
  }

  PG_RETURN_VOID();
}

Datum teaprotocol_export(PG_FUNCTION_ARGS) { elog(ERROR, "Tea error: Writeable tables are not supported"); }

typedef struct {
  Import *import;
  FunctionCallInfo fcinfo;
} FetchRowContext;

static void DestroyScanParams(ExternalScanParams *params) {
  if (params) {
    pfree(params->projection.columns);
    // Formally, allocated filter string should be also freed, but
    // call pfree after pstrdup will lead to crash. So comment the line out.
    // pfree((char *)params->filter);
    pfree(params);
  }
}

static ExternalScanParams *MakeScanParams(const Import *context, const char *url, struct ExternalSelectDescData *desc,
                                          TupleDesc tupdesc, IgnoredExprs ignored_exprs) {
  assert(context->columns);

  ExternalScanParams *params = palloc0(sizeof(ExternalScanParams));
  params->session_id = context->session_id;
  params->segment_id = GpIdentity.segindex;
  params->segment_count = getgpsegmentCount();
  params->filter = ConvertPGClausesToTeaNodes(kExternal, desc->filter_quals, NULL, NULL, tupdesc, ignored_exprs);
  params->projection = MakeScanProjection(tupdesc, context->ncolumns, context->columns, NULL);
  params->slice_id = currentSliceId;
  return params;
}

/*
 * In order to establish rich communication between Protocol handler and Formatter the data buffer is used.
 *
 * The formatter expects the input buffer to be filled with TeaHandshake struct. The data from the buffer is consumed
 * only after all the required tuples have been returned.
 *
 * Since Greenplum 5 doesn't use MVCC semantics while initializin an external there is a race condition where custom
 * protocol UDF and custom formatter UDF might come from different version of the extension module. In order to be
 * consistent in behaviour the formatter function is as small as possible and doesn't call into any C++ code directly.
 * Instead it forwards the calls into the callback provided by the protocol handler.
 */

#define TEA_PROTOCOL_VERSION 2

typedef struct {
  Datum (*fetch_next_tuple_callback)(PG_FUNCTION_ARGS);
} TeaFormatterData;

typedef struct {
  TeaFormatterData formatter_data;
  bool formatter_initialized;
  Import import;
} TeaProtocolData;

typedef struct {
  int version;
  TeaProtocolData *context;
} TeaHandshake;

static_assert(sizeof(TeaHandshake) != sizeof(void *),
              "Size of TeaHandshake struct is the same as unversioned handshake");
static_assert(offsetof(TeaProtocolData, formatter_data) == 0,
              "The formatter_data member is not the first member of TeaProtocolData");

#define TEA_HANDSHAKE_LEN ((int)sizeof(TeaHandshake))

static int StoreContextToProtocolData(FunctionCallInfo protocol_fcinfo, TeaProtocolData *context) {
  char *databuf = EXTPROTOCOL_GET_DATABUF(protocol_fcinfo);
  int datalen = EXTPROTOCOL_GET_DATALEN(protocol_fcinfo);
  if (datalen < TEA_HANDSHAKE_LEN) {
    elog(ERROR, "Tea error: teaprotocol_import. Data buffer is too small: %d", datalen);
  }
  TeaHandshake handshake;
  handshake.version = TEA_PROTOCOL_VERSION;
  handshake.context = context;
  memcpy(databuf, &handshake, TEA_HANDSHAKE_LEN);
  return TEA_HANDSHAKE_LEN;
}

static TeaFormatterData *LoadContextFromFormatterData(FunctionCallInfo formatter_fcinfo) {
  char *data_buf = FORMATTER_GET_DATABUF(formatter_fcinfo);
  int data_len = FORMATTER_GET_DATALEN(formatter_fcinfo);
  if (data_len != TEA_HANDSHAKE_LEN) {
    elog(ERROR, "Tea error: unexpected formatter data len %d", data_len);
  }
  int data_cur = FORMATTER_GET_DATACURSOR(formatter_fcinfo);
  if (data_cur != 0) {
    elog(ERROR, "Tea error: unexpected formatter data cursor %d", data_cur);
  }
  TeaHandshake handshake;
  memcpy(&handshake, data_buf, TEA_HANDSHAKE_LEN);
  if (handshake.version != TEA_PROTOCOL_VERSION) {
    elog(ERROR, "Tea error: custom formatter version is not compatible with the custom protocol");
  }
  if (handshake.context == NULL) {
    elog(ERROR, "Tea error: formatter expects data from tea custom protocol");
  }
  return &handshake.context->formatter_data;
}

static void ConsumeFormatterData(FunctionCallInfo formatter_fcinfo) {
  FORMATTER_SET_DATACURSOR(formatter_fcinfo, TEA_HANDSHAKE_LEN);
}

static void DestroyProtocolData(TeaProtocolData *context) {
  if (context) {
    if (context->import.tea_ctx) {
      TeaContextDestroy(context->import.tea_ctx);
    }
    pfree(context->import.columns);
    pfree(context);
  }
}

static void InitImportContext(Import *context, char *url, TupleDesc tupdesc, struct ExternalSelectDescData *desc) {
  char session_id[SESSION_ID_LEN];
  GetScanSessionId(session_id, SESSION_ID_LEN);
  memcpy(context->session_id, session_id, SESSION_ID_LEN);
  context->columns = (int *)palloc0(sizeof(int) * tupdesc->natts);
  context->values = palloc0(sizeof(Datum) * tupdesc->natts);
  context->nulls = palloc0(sizeof(bool) * tupdesc->natts);
  context->tea_ctx = TeaContextCreate(url);
  TeaContextGetOptions(context->tea_ctx, &context->options);
  context->ncolumns = GetRequiredColumns(desc, context->columns, tupdesc->natts,
                                         context->options.ext_table_filter_walker_for_projection);
  InitHeapFormTupleInfo(&context->heap_form_tuple_info, context->columns, context->ncolumns);
  ExternalScanParams *params = MakeScanParams(context, url, desc, tupdesc, context->options.ignored_exprs);
  if (params->filter.all_extracted) {
    elog(LOG, "Using filter %s", params->filter.all_extracted);
  }
  TeaContextPlanExternal(context->tea_ctx, params);
  DestroyScanParams(params);
}

Datum teaprotocol_import(PG_FUNCTION_ARGS) {
  if (!CALLED_AS_EXTPROTOCOL(fcinfo)) {
    elog(ERROR, "Tea error: teaprotocol_import not called by protocol mgr");
  }

  TeaProtocolData *context = (TeaProtocolData *)EXTPROTOCOL_GET_USER_CTX(fcinfo);

  if (EXTPROTOCOL_IS_LAST_CALL(fcinfo)) {
    elog(DEBUG1, "teaprotocol_import. last call");
    if (context) {
      EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
      DestroyProtocolData(context);
    }
    PG_RETURN_INT32(0);
  }

  if (context == NULL) {
    context = palloc0(sizeof(TeaProtocolData));
    context->formatter_data.fetch_next_tuple_callback = &FetchNextTuple;
    context->formatter_initialized = false;
    InitImportContext(&context->import, EXTPROTOCOL_GET_URL(fcinfo), RelationGetDescr(EXTPROTOCOL_GET_RELATION(fcinfo)),
                      EXTPROTOCOL_GET_EXTERNAL_SELECT_DESC(fcinfo));
    EXTPROTOCOL_SET_USER_CTX(fcinfo, context);
    PG_RETURN_INT32(StoreContextToProtocolData(fcinfo, context));
  }

  PG_RETURN_INT32(0);
}

static void FetchRow(FetchRowContext *context, Datum *values, bool *nulls) {
  const FormatterData *formatter_data = (const FormatterData *)context->fcinfo->context;
  const TupleDesc tupdesc = FORMATTER_GET_TUPDESC(context->fcinfo);
  const bool needs_transcoding = formatter_data->fmt_needs_transcoding;

  memset(nulls, true, tupdesc->natts);
  TeaContextFetchTuple(context->import->tea_ctx, needs_transcoding ? formatter_data->fmt_conversion_proc : NULL, values,
                       nulls);
}

Datum teaformat_import(PG_FUNCTION_ARGS) {
  if (!CALLED_AS_FORMATTER(fcinfo)) {
    elog(ERROR, "Tea error: teaformat_import not called by formatter");
  }

  TeaFormatterData *formatter_data = (TeaFormatterData *)FORMATTER_GET_USER_CTX(fcinfo);
  if (formatter_data == NULL) {
    elog(DEBUG1, "teaformat_import first call");

    formatter_data = LoadContextFromFormatterData(fcinfo);
    FORMATTER_SET_USER_CTX(fcinfo, formatter_data);

    elog(DEBUG1, "import_format initialized");
  }
  return formatter_data->fetch_next_tuple_callback(fcinfo);
}

static Datum FetchNextTuple(PG_FUNCTION_ARGS) {
  TeaProtocolData *protocol_data = (TeaProtocolData *)FORMATTER_GET_USER_CTX(fcinfo);
  if (protocol_data == NULL) {
    elog(ERROR, "Unexpected empty formatter user context.");
  }
  if (!protocol_data->formatter_initialized) {
    int ext_encoding = FORMATTER_GET_EXTENCODING(fcinfo);
    if (ext_encoding != PG_UTF8) {
      elog(ERROR, "Tea error: external table encoding should be set to UTF8 (got %d)", ext_encoding);
    }
    protocol_data->formatter_initialized = true;
  }

  Import *import = &protocol_data->import;
  TupleDesc tupdesc = FORMATTER_GET_TUPDESC(fcinfo);
  FetchRowContext context;
  context.fcinfo = fcinfo;
  context.import = import;

  bool has_row;
  TeaContextPrepareTuple(import->tea_ctx, &has_row);
  CHECK_FOR_INTERRUPTS();

  if (!has_row) {
    elog(DEBUG1, "we are asked to stop processing");
    TeaContextLogStats(import->tea_ctx, "EXT_TABLE_QUERY");
    ConsumeFormatterData(fcinfo);
    FORMATTER_RETURN_NOTIFICATION(fcinfo, FMT_NEED_MORE_DATA);
  }

  MemoryContext m = FORMATTER_GET_PER_ROW_MEM_CTX(fcinfo);
  MemoryContext oldcontext = MemoryContextSwitchTo(m);
  /* clang-format off */
  PG_TRY();
    FetchRow(&context, import->values, import->nulls);
  PG_CATCH();
    MemoryContextSwitchTo(oldcontext);
    PG_RE_THROW();
  PG_END_TRY();
  /* clang-format on */
  MemoryContextSwitchTo(oldcontext);
  int64_t start = MeasureTicks();
  HeapTuple tuple;
  if (context.import->options.use_custom_heap_form_tuple) {
    tuple = TeaHeapFormTuple(tupdesc, import->values, import->nulls, &import->heap_form_tuple_info);
  } else {
    tuple = heap_form_tuple(tupdesc, import->values, import->nulls);
  }
  int64_t end = MeasureTicks();
  UpdateExtStats(import, start, end);
  FORMATTER_SET_TUPLE(fcinfo, tuple);
  FORMATTER_RETURN_TUPLE(tuple);
}

Datum teaformat_export(PG_FUNCTION_ARGS) { elog(ERROR, "Tea error: Writeable tables are not supported"); }
