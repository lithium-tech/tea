#pragma once

#include "iceberg/tea_column_stats.h"

#include "tea/filter/gp/serialized_filter.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

#include "tea/gpext/tea_column.h"
#include "tea/observability/ext_stats.h"
#include "tea/table/gp_fwd.h"

typedef void *InternalContextPtr;

typedef struct TeaContext {
  InternalContextPtr ctx;

  ExtStats ext_stats;
} TeaContext;

typedef TeaContext *TeaContextPtr;

typedef struct {
  const char *session_id;
  int segment_id;
  int segment_count;
  ReaderScanProjection projection;
  CSerializedFilter filter;
  int32_t slice_id;
} ExternalScanParams;

typedef struct {
  const char *session_id;
  int segment_id;
  int segment_count;
  ReaderScanProjection projection;
  const char *metadata;
  CSerializedFilter filter;
  bool postfilter_on_gp;
  int32_t slice_id;
} ForeignScanParams;

typedef struct {
  const char *session_id;
  ReaderScanProjection projection;
  const char *metadata;
} AnalyzeParams;

typedef struct ReaderRelationSize {
  double rows;
  int width;
} ReaderRelationSize;

typedef struct ReaderOptions {
  bool use_custom_heap_form_tuple;
  bool ext_table_filter_walker_for_projection;
  bool use_virtual_tuple;
  bool postfilter_on_gp;
  IgnoredExprs ignored_exprs;
} ReaderOptions;

/**
 * Initialize shared state of the library.
 */
void TeaContextInitialize(int db_encoding);

/**
 * Finalize shared state.
 */
void TeaContextFinalize();

/**
 * Create reader instance.
 */
TeaContextPtr TeaContextCreate(const char *url);

/**
 * Create reader instance. Does not to checks for initialization or tracks the instance for automatic deinitialization.
 */
TeaContextPtr TeaContextCreateUntracked(const char *url);

/**
 * Destroy reader instance.
 */
void TeaContextDestroy(TeaContextPtr tea_ctx);

/**
 * Destroy reader instance. Does not updates reader tracking information.
 */
void TeaContextDestroyUntracked(TeaContextPtr tea_ctx);

/**
 * Initialize reading cursor. Match Greenplum schema with Iceberg.
 */
void TeaContextPlanExternal(TeaContextPtr tea_ctx, const ExternalScanParams *params);
void TeaContextPlanForeign(TeaContextPtr tea_ctx, const ForeignScanParams *params);
void TeaContextPlanAnalyze(TeaContextPtr tea_ctx, const AnalyzeParams *params);

/**
 * Make next tuple avaliable for reading.
 */
void TeaContextPrepareTuple(TeaContextPtr tea_ctx, bool *has_next);
/**
 * Skip next tuple without reading.
 */
void TeaContextSkipTuple(const TeaContextPtr tea_ctx);
/**
 * Read next tuple.
 */
void TeaContextFetchTuple(const TeaContextPtr tea_ctx, struct FmgrInfo *fcinfo, GpDatum *values, bool *isnull);

/**
 * Get estimated size of the relation
 */
void TeaContextGetRelationSize(const TeaContextPtr tea_ctx, const char *session_id, const ReaderScanProjection *proj,
                               ReaderRelationSize *relsize);

/**
 * Get table metadata from Iceberg in form of serialized
 * MetadataResponseResult message.
 */
void TeaContextGetScanMetadata(const TeaContextPtr tea_ctx, const char *session_id, const char *file_filter,
                               char **metadata, int segment_count);

void TeaContextGetIcebergColumnStats(const char *url, const char *session_id, const char *column_name,
                                     ColumnStats *result);

void TeaContextGetOptions(TeaContextPtr tea_ctx, ReaderOptions *options);

/**
 * Format stats in the format accepted by the monitoring system.
 */
void TeaContextLogStats(const TeaContextPtr tea_ctx, const char *event);

#ifdef __cplusplus
}
#endif
