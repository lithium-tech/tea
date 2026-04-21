#include <assert.h>
#include <inttypes.h>
#include <stdint.h>

#include "postgres.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/namespace.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "storage/itemptr.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/rel.h"

#include "tea/filter/gp/convert.h"
#include "tea/gpext/tea_column.h"
#include "tea/gpext/tea_common.h"
#include "tea/gpext/tea_fdw_options.h"
#include "tea/gpext/tea_fdw_proj.h"
#include "tea/gpext/tea_import.h"
#include "tea/gpext/tea_reader.h"
#include "tea/gpext/tea_tuple.h"
#include "tea/util/measure_c.h"

PG_FUNCTION_INFO_V1(tea_fdw_handler);
PG_FUNCTION_INFO_V1(tea_fdw_validator);

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * We store various information in ForeignScan.fdw_private to pass it from
 * planner to executor.  Currently we store:
 *
 * 1) SELECT statement text to be sent to the remote server
 * 2) Integer list of attribute numbers retrieved by the SELECT
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex {
  /* Integer list of attribute numbers retrieved by the SELECT and attribute
     numbers used in local conditions */
  FdwScanPrivateRetrievedAttrs,
  /* Integer list of attribute numbers retrieved by the SELECT and attribute
     numbers used in local and remote conditions */
  FdwScanPrivateUsedAttrs,
  FdwScanPrivateMetadata,
  /* SQL statement to execute remotely on row level (as a String
     node) */
  FdwScanPrivateCommonFilter,
  FdwScanPrivateRowFilter,
  FdwScanPrivateMustUseRowFilter,
};

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct TeaRelationInfo {
  /* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
  List* remote_conds;
  List* local_conds;
  char* iceberg_metadata;
  CSerializedFilter filter;
  bool postfilter_on_gp;

  /// Bitmap of attr numbers we need to fetch from the remote server.
  Bitmapset* retrieved_attrs_bitmap;
  /// Bitmap of attr numbers we need to read on remote server.
  Bitmapset* used_attrs_bitmap;

  /* Cost and selectivity of local_conds. */
  QualCost local_conds_cost;
  Selectivity local_conds_sel;

  /* Estimated size and cost for a scan with baserestrictinfo quals. */
  double rows;
  int width;
  Cost startup_cost;
  Cost total_cost;

  /// List of attributes (columns) that we need to get.
  List* retrieved_attrs;
  /// List of attributes (columns) that remote need to process.
  List* used_attr;

  /* Cached catalog information. */
  TupleDesc desc;
  char* location;
  ForeignTable* table;
  ForeignServer* server;
} TeaRelationInfo;

/*
 * Execution state of a foreign scan using postgres_fdw.
 */
typedef struct TeaScanState {
  /// relcache entry for the foreign table.
  Relation rel;
  /// Attribute datatype conversion metadata.
  AttInMetadata* attinmeta;

  /* extracted fdw_private data */

  char* iceberg_metadata;
  CSerializedFilter filter;
  bool postfilter_on_gp;
  /// List of attribute numbers used on remote.
  List* used_attrs;
  /// List of retrieved attribute numbers.
  List* retrieved_attrs;
  List* remote_clause;

  /* for remote query execution */
  char* location;
  Import import;
  bool* is_remote_only;
  FmgrInfo* fmt_conversion_proc;
  ItemPointerData fake_ctid;

  /* Working memory contexts */

  /// Context holding current batch of tuples.
  MemoryContext batch_cxt;
  /// Context for per-tuple temporary data.
  MemoryContext temp_cxt;

  bool use_virtual_tuple;
} TeaScanState;

static void DestroyScanParams(ForeignScanParams* params) {
  if (params) {
    pfree(params->projection.columns);
    // Formally, allocated filter string should be also freed, but
    // call pfree after pstrdup will lead to crash. So comment the line out.
    // pfree((char *)params->filter);
    pfree(params);
  }
}

static void DestroyAnalyzeParams(AnalyzeParams* params) {
  if (params) {
    pfree(params->projection.columns);
    // Formally, allocated filter string should be also freed, but
    // call pfree after pstrdup will lead to crash. So comment the line out.
    // pfree((char *)params->filter);
    pfree(params);
  }
}

///////////////////////////////////////////////////////////////////////////////

/*
 * Get cost and size estimates for a foreign scan
 *
 * We assume that all the baserestrictinfo clauses will be applied, plus
 * any join clauses listed in join_conds.
 */
static void EstimatePathCostSize(PlannerInfo* root, RelOptInfo* baserel, List* join_conds, double* p_rows, int* p_width,
                                 Cost* p_startup_cost, Cost* p_total_cost) {
  TeaRelationInfo* fpinfo = (TeaRelationInfo*)baserel->fdw_private;
  double rows;
  double retrieved_rows;
  int width;
  Cost startup_cost;
  Cost total_cost;
  Cost run_cost;
  Cost cpu_per_tuple;

  /*
   * We don't support join conditions in this mode (hence, no
   * parameterized paths can be made).
   */
  Assert(join_conds == NIL);

  /* Use rows/width estimates made by set_baserel_size_estimates. */
  rows = baserel->rows;
  width = baserel->width;

  /*
   * Back into an estimate of the number of retrieved rows.  Just in
   * case this is nuts, clamp to at most baserel->tuples.
   */
  retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
  retrieved_rows = Min(retrieved_rows, baserel->tuples);

  /*
   * Cost as though this were a seqscan, which is pessimistic.  We
   * effectively imagine the local_conds are being evaluated remotely,
   * too.
   */
  startup_cost = 0;
  run_cost = 0;
  run_cost += seq_page_cost * baserel->pages;

  startup_cost += baserel->baserestrictcost.startup;
  cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
  run_cost += cpu_per_tuple * baserel->tuples;

  total_cost = startup_cost + run_cost;

  /*
   * Add some additional cost factors to account for connection overhead
   * (fdw_startup_cost), transferring data across the network
   * (fdw_tuple_cost per retrieved row), and local manipulation of the data
   * (cpu_tuple_cost per retrieved row).
   */
  // startup_cost += fpinfo->fdw_startup_cost;
  // total_cost += fpinfo->fdw_startup_cost;
  // total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
  total_cost += cpu_tuple_cost * retrieved_rows;

  /* Return results. */
  *p_rows = rows;
  *p_width = width;
  *p_startup_cost = startup_cost;
  *p_total_cost = total_cost;

  elog(LOG, "EstimatePathCostSize: p_rows = %f, p_width = %d, p_startup_cost = %f, p_total_cost = %f", *p_rows,
       *p_width, *p_startup_cost, *p_total_cost);
}

static int GetRequiredColumns(const List* used_attrs, const List* retrieved_attrs, TupleDesc tupdesc, bool* remote_only,
                              int* columns, const int ncolumns);

/*
 * Obtain relation size estimates for a foreign table
 */
static void TeaGetForeignRelSize(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid) {
  elog(DEBUG1, "! TeaGetForeignRelSize");

  char session_id[SESSION_ID_LEN];
  // We use TeaRelationInfo to pass various information to subsequent
  // functions.
  TeaRelationInfo* fpinfo = (TeaRelationInfo*)palloc0(sizeof(TeaRelationInfo));
  baserel->fdw_private = (void*)fpinfo;

  GetScanSessionId(session_id, SESSION_ID_LEN);

  // Look up foreign-table catalog info.
  fpinfo->table = GetForeignTable(foreigntableid);
  fpinfo->server = GetForeignServer(fpinfo->table->serverid);

  /*
   * Core code already has some lock on each rel being planned, so we can
   * use NoLock here.
   */
  RangeTblEntry* rte = planner_rt_fetch(baserel->relid, root);
  Relation rel;

#if PG_VERSION_NUM >= 90600
  rel = table_open(rte->relid, NoLock);
#else
  rel = heap_open(rte->relid, NoLock);
#endif
  fpinfo->location = TeaGetLocation(RelationGetRelid(rel));

  TeaContextPtr reader = TeaContextCreate(fpinfo->location);
  ReaderOptions options;
  TeaContextGetOptions(reader, &options);
  fpinfo->postfilter_on_gp = options.postfilter_on_gp;

  // Identify which baserestrictinfo clauses can be sent to the remote
  // server and which can't.

  fpinfo->filter = ConvertPGClausesToTeaNodes(kForeign, baserel->baserestrictinfo, &fpinfo->remote_conds,
                                              &fpinfo->local_conds, RelationGetDescr(rel), options.ignored_exprs);

  /*
   * Compute the selectivity and cost of the local_conds, so we don't have
   * to do it over again for each path.  The best we can do for these
   * conditions is to estimate selectivity on the basis of local statistics.
   */
  fpinfo->local_conds_sel = clauselist_selectivity(root, fpinfo->local_conds, baserel->relid, JOIN_INNER, NULL, false);

  cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

  // Identify which attributes will need to be retrieved from the remote server.
  fpinfo->retrieved_attrs_bitmap = GetUsedAttributesSet(baserel, fpinfo->local_conds);
  fpinfo->used_attrs_bitmap = GetUsedAttributesSet(baserel, baserel->baserestrictinfo);

  // Create an integer List of the columns being retrieved.
  if (!options.postfilter_on_gp) {
    DeparseTargetList(rel, fpinfo->retrieved_attrs_bitmap, &fpinfo->retrieved_attrs);
    DeparseTargetList(rel, fpinfo->used_attrs_bitmap, &fpinfo->used_attr);
  } else {
    DeparseTargetList(rel, fpinfo->used_attrs_bitmap, &fpinfo->retrieved_attrs);
    DeparseTargetList(rel, fpinfo->used_attrs_bitmap, &fpinfo->used_attr);
  }

  fpinfo->desc = RelationGetDescr(rel);

  heap_close(rel, NoLock);

  const char* extracted_filter = fpinfo->filter.all_extracted;
  TeaContextGetScanMetadata(reader, session_id, extracted_filter, &fpinfo->iceberg_metadata, getgpsegmentCount());

  // TODO(gmusya): retrieve and set number of rows if possible
  {
    /*
     * If the foreign table has never been ANALYZEd, it will have relpages
     * and reltuples equal to zero, which most likely has nothing to do
     * with reality.  We can't do a whole lot about that if we're not
     * allowed to consult the remote server, but we can use a hack similar
     * to plancat.c's treatment of empty relations: use a minimum size
     * estimate of 10 pages, and divide by the column-datatype-based width
     * estimate to get the corresponding number of tuples.
     */
    if (baserel->pages == 0 && baserel->tuples == 0) {
      baserel->pages = 10;
      baserel->tuples = (10 * BLCKSZ) / (baserel->width + sizeof(HeapTupleHeaderData));
    }

    /* Estimate baserel size as best we can with local statistics. */
    set_baserel_size_estimates(root, baserel);

    /* Fill in basically-bogus cost estimates for use later. */
    EstimatePathCostSize(root, baserel, NIL, &fpinfo->rows, &fpinfo->width, &fpinfo->startup_cost, &fpinfo->total_cost);
  }

  TeaContextLogStats(reader, "FDW_PLAN");
  TeaContextDestroy(reader);
}

/*
 * Create possible access paths for a scan on the foreign table
 *
 * Currently we don't support any push-down feature, so there is only one
 * possible access path, which simply returns all records in the order in
 * the data file.
 */
static void TeaGetForeignPaths(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid) {
  elog(DEBUG1, "! TeaGetForeignPaths");

  TeaRelationInfo* fpinfo = (TeaRelationInfo*)baserel->fdw_private;
  ForeignPath* path;

  /*
   * Create simplest ForeignScan path node and add it to baserel.  This path
   * corresponds to SeqScan path of regular tables (though depending on what
   * baserestrict conditions we were able to send to remote, there might
   * actually be an indexscan happening there).  We already did all the work
   * to estimate cost and size of this path.
   *
   * Although this path uses no join clauses, it could still have required
   * parameterization due to LATERAL refs in its tlist.
   */
  path = create_foreignscan_path(root, baserel, fpinfo->rows, fpinfo->startup_cost, fpinfo->total_cost,
                                 NIL,  /* no pathkeys */
                                 NULL, /* no outer rel either */
                                 fpinfo->retrieved_attrs);
  add_path(baserel, (Path*)path);

  /*
   * If we're not using remote estimates, stop here.  We have no way to
   * estimate whether any join clauses would be worth sending across, so
   * don't bother building parameterized paths.
   */
  return;
}

/*
 * Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan* TeaGetForeignPlan(PlannerInfo* root, RelOptInfo* baserel, Oid foreigntableid,
                                      ForeignPath* best_path, List* tlist, List* scan_clauses) {
  elog(DEBUG1, "! TeaGetForeignPlan");

  TeaRelationInfo* fpinfo = (TeaRelationInfo*)baserel->fdw_private;
  Index scan_relid = baserel->relid;
  List* fdw_private;
  ListCell* lc;
  List* remote_expr = NIL;
  List* local_exprs = NIL;

  if (!fpinfo->postfilter_on_gp) {
    /*
     * Separate the scan_clauses into those that can be executed remotely and
     * those that can't.  baserestrictinfo clauses that were previously
     * determined to be safe or unsafe by classifyConditions are shown in
     * fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
     * scan_clauses list will be a join clause, which we have to check for
     * remote-safety.
     *
     * Note: the join clauses we see here should be the exact same ones
     * previously examined by postgresGetForeignPaths.  Possibly it'd be worth
     * passing forward the classification work done then, rather than
     * repeating it here.
     *
     * This code must match "extract_actual_clauses(scan_clauses, false)"
     * except for the additional decision about remote versus local execution.
     * Note however that we only strip the RestrictInfo nodes from the
     * local_exprs list, since appendWhereClause expects a list of
     * RestrictInfos.
     */
    foreach (lc, scan_clauses) {
      RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

      Assert(IsA(rinfo, RestrictInfo));

      /* Ignore any pseudoconstants, they're dealt with elsewhere */
      if (rinfo->pseudoconstant) continue;

      if (list_member_ptr(fpinfo->remote_conds, rinfo)) {
        remote_expr = lappend(remote_expr, rinfo->clause);
      } else if (list_member_ptr(fpinfo->local_conds, rinfo)) {
        local_exprs = lappend(local_exprs, rinfo->clause);
      } else {
        local_exprs = lappend(local_exprs, rinfo->clause);
      }
    }

    // TODO(artpaul): pass remote_conds to iceberg reader.
  } else {
    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.  So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    local_exprs = extract_actual_clauses(scan_clauses, false);
    remote_expr = local_exprs;
  }

#define list_make5(x1, x2, x3, x4, x5) lcons(x1, list_make4(x2, x3, x4, x5))
#define list_make6(x1, x2, x3, x4, x5, x6) lcons(x1, list_make5(x2, x3, x4, x5, x6))
  /*
   * Build the fdw_private list that will be available to the executor.
   * Items in the list must match enum FdwScanPrivateIndex, above.
   */
  fdw_private = list_make6(fpinfo->retrieved_attrs, fpinfo->used_attr, makeString(fpinfo->iceberg_metadata),
                           makeString(fpinfo->filter.all_extracted), makeString(fpinfo->filter.row),
                           makeInteger(fpinfo->postfilter_on_gp));

  /* Create the ForeignScan node */
  return make_foreignscan(tlist, local_exprs, scan_relid, NIL, /* no expressions to evaluate */
                          fdw_private);
}

/*
 * Report projection description to the remote component
 */
static int GetRequiredColumns(const List* used_attrs, const List* retrieved_attrs, TupleDesc tupdesc, bool* remote_only,
                              int* columns, const int ncolumns) {
  int* out = columns;

  if (used_attrs == NIL) {
    return out - columns;
  } else {
    ListCell* lc = NULL;
    Bitmapset* used_attrs_projected = NULL;
    Bitmapset* retrieved_attrs_projected = NULL;

    foreach (lc, used_attrs) {
      int attno = lfirst_int(lc);

      used_attrs_projected = bms_add_member(used_attrs_projected, attno - FirstLowInvalidHeapAttributeNumber);
    }
    foreach (lc, retrieved_attrs) {
      int attno = lfirst_int(lc);

      retrieved_attrs_projected = bms_add_member(retrieved_attrs_projected, attno - FirstLowInvalidHeapAttributeNumber);
    }
    for (int i = 1; i <= ncolumns; i++) {
      if (tupdesc->attrs[i - 1]->attisdropped) {
        continue;
      }

      if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, retrieved_attrs_projected)) {
        *out++ = i - 1;
        *remote_only++ = false;
      } else if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, used_attrs_projected)) {
        /* zero-based index in the server side */
        *out++ = i - 1;
        *remote_only++ = true;
      }
    }
    bms_free(used_attrs_projected);
    bms_free(retrieved_attrs_projected);
  }

  return out - columns;
}

static ForeignScanParams* MakeScanParams(const TeaScanState* fsstate, const char* url) {
  assert(fsstate->import.columns);

  TupleDesc tupdesc = fsstate->attinmeta->tupdesc;
  ForeignScanParams* params = palloc0(sizeof(ForeignScanParams));
  params->session_id = fsstate->import.session_id;
  params->segment_id = GpIdentity.segindex;
  params->segment_count = getgpsegmentCount();
  params->metadata = fsstate->iceberg_metadata;
  params->filter = fsstate->filter;
  params->postfilter_on_gp = fsstate->postfilter_on_gp;
  params->projection =
      MakeScanProjection(tupdesc, fsstate->import.ncolumns, fsstate->import.columns, fsstate->is_remote_only);

  params->slice_id = currentSliceId;
  return params;
}

static AnalyzeParams* MakeAnalyzeParams(const TeaScanState* fsstate, const char* url) {
  assert(fsstate->import.columns);

  TupleDesc tupdesc = fsstate->attinmeta->tupdesc;
  AnalyzeParams* params = palloc0(sizeof(AnalyzeParams));
  params->session_id = fsstate->import.session_id;
  params->metadata = fsstate->iceberg_metadata;
  params->projection =
      MakeScanProjection(tupdesc, fsstate->import.ncolumns, fsstate->import.columns, fsstate->is_remote_only);

  return params;
}

/*
 * Initiate access to the table by creating CopyState
 */
static void TeaBeginForeignScan(ForeignScanState* node, int eflags) {
  elog(DEBUG1, "! TeaBeginForeignScan");

  // Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
    return;
  }

  ForeignScan* fsplan = (ForeignScan*)node->ss.ps.plan;
  EState* estate = node->ss.ps.state;
  TeaScanState* fsstate;
  int ncolumns;
  char session_id[SESSION_ID_LEN];

  /*
   * We'll save private state in node->fdw_state.
   */
  fsstate = (TeaScanState*)palloc0(sizeof(TeaScanState));
  node->fdw_state = (void*)fsstate;

  /* Get info about foreign table. */
  fsstate->rel = node->ss.ss_currentRelation;
  fsstate->location = TeaGetLocation(RelationGetRelid(node->ss.ss_currentRelation));

  /* Get private info created by planner functions. */
  fsstate->iceberg_metadata = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateMetadata));
  fsstate->used_attrs = (List*)list_nth(fsplan->fdw_private, FdwScanPrivateUsedAttrs);
  fsstate->retrieved_attrs = (List*)list_nth(fsplan->fdw_private, FdwScanPrivateRetrievedAttrs);
  fsstate->filter.all_extracted = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateCommonFilter));
  fsstate->filter.row = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateRowFilter));
  fsstate->postfilter_on_gp = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateMustUseRowFilter));

  /* Create contexts for per-tuple temp workspace. */
  fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt, "tea_fdw temporary data", ALLOCSET_SMALL_MINSIZE,
                                            ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);

  /* Get info we'll need for input data conversion. */
  fsstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(fsstate->rel));
  ncolumns = fsstate->attinmeta->tupdesc->natts;

  Import* import = &fsstate->import;

  import->columns = (int*)palloc0(sizeof(int) * ncolumns);
  fsstate->is_remote_only = (bool*)palloc0(sizeof(bool) * ncolumns);
  import->ncolumns = GetRequiredColumns(fsstate->used_attrs, fsstate->retrieved_attrs, fsstate->attinmeta->tupdesc,
                                        fsstate->is_remote_only, import->columns, ncolumns);

  import->values = palloc0(sizeof(Datum) * ncolumns);
  import->nulls = palloc0(sizeof(bool) * ncolumns);

  GetScanSessionId(session_id, SESSION_ID_LEN);
  memcpy(fsstate->import.session_id, session_id, SESSION_ID_LEN);

  if (GetDatabaseEncoding() != PG_UTF8) {
    Oid conversion_proc = FindDefaultConversionProc(PG_UTF8, GetDatabaseEncoding());
    if (OidIsValid(conversion_proc)) {
      FmgrInfo* enc_conversion_proc = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
      fmgr_info(conversion_proc, enc_conversion_proc);
      fsstate->fmt_conversion_proc = enc_conversion_proc;
    }
  }
}

/*
 * Fetch some more rows from the node's cursor.
 */
static HeapTuple FetchData(TeaScanState* fsstate, MemoryContext tuple_ctx) {
  TupleDesc tupdesc = fsstate->attinmeta->tupdesc;
  MemoryContext oldcontext;
  HeapTuple tuple = NULL;

  PG_TRY();
  {
    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(fsstate->temp_cxt);

    memset(fsstate->import.nulls, true, tupdesc->natts);
    TeaContextFetchTuple(fsstate->import.tea_ctx, fsstate->fmt_conversion_proc, fsstate->import.values,
                         fsstate->import.nulls);

    /* Build the result tuple in batch context. */
    MemoryContextSwitchTo(oldcontext);

    oldcontext = MemoryContextSwitchTo(tuple_ctx);

    int64_t start = MeasureTicks();
    if (fsstate->import.options.use_custom_heap_form_tuple) {
      tuple = TeaHeapFormTuple(tupdesc, fsstate->import.values, fsstate->import.nulls,
                               &fsstate->import.heap_form_tuple_info);
    } else {
      tuple = heap_form_tuple(tupdesc, fsstate->import.values, fsstate->import.nulls);
    }
    int64_t end = MeasureTicks();
    UpdateExtStats(&fsstate->import, start, end);

    MemoryContextSwitchTo(oldcontext);

    /* Clean up temporary. */
    MemoryContextReset(fsstate->temp_cxt);
  }
  PG_CATCH();
  PG_RE_THROW();
  PG_END_TRY();

  return tuple;
}

static void FetchDataToVirtualTuple(TeaScanState* fsstate, MemoryContext tuple_ctx, Datum* values, bool* nulls) {
  TupleDesc tupdesc = fsstate->attinmeta->tupdesc;
  MemoryContext oldcontext;
  PG_TRY();
  {
    /*
     * Do the following work in a temp context that we reset after each tuple.
     * This cleans up not only the data we have direct access to, but any
     * cruft the I/O functions might leak.
     */
    oldcontext = MemoryContextSwitchTo(fsstate->temp_cxt);

    memset(nulls, true, tupdesc->natts);
    TeaContextFetchTuple(fsstate->import.tea_ctx, fsstate->fmt_conversion_proc, values, nulls);

    /* Build the result tuple in batch context. */
    MemoryContextSwitchTo(oldcontext);

    /* Clean up temporary. */
    MemoryContextReset(fsstate->temp_cxt);
  }
  PG_CATCH();
  PG_RE_THROW();
  PG_END_TRY();

  return;
}

/*
 * Read next record from the data file and store it into the
 * ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot* TeaIterateForeignScan(ForeignScanState* node) {
  TeaScanState* fsstate = (TeaScanState*)node->fdw_state;
  ForeignScan* fsplan = (ForeignScan*)node->ss.ps.plan;
  TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
  Import* import = &fsstate->import;
  if (!import->tea_ctx) {
    /*
     * Get connection to the foreign server.  Connection manager will
     * establish new connection if necessary.
     */
    import->tea_ctx = TeaContextCreate(fsstate->location);
    TeaContextGetOptions(import->tea_ctx, &import->options);
    if (import->options.use_custom_heap_form_tuple) {
      InitHeapFormTupleInfo(&import->heap_form_tuple_info, import->columns, import->ncolumns);
    }
    ForeignScanParams* params = MakeScanParams(fsstate, fsstate->location);
    if (params->filter.all_extracted) {
      elog(LOG, "Using filter %s", params->filter.all_extracted);
    }
    if (params->metadata) {
      elog(LOG, "Using provided metadata of size %d", (int)strlen(params->metadata));
    }

    TeaContextPlanForeign(import->tea_ctx, params);
    DestroyScanParams(params);
  }

  bool has_row;
  TeaContextPrepareTuple(import->tea_ctx, &has_row);
  CHECK_FOR_INTERRUPTS();

  if (has_row) {
    if (fsplan->fsSystemCol || !fsstate->use_virtual_tuple) {
      HeapTuple tuple = FetchData(fsstate, slot->tts_mcxt);
      if (tuple) {
        ExecStoreHeapTuple(tuple, slot, InvalidBuffer, true);
        // https://<>/gpdb/-/blob/6X_STABLE/src/backend/executor/nodeExternalscan.c#L110
        if (fsplan->fsSystemCol) {
          slot_set_ctid_from_fake(slot, &fsstate->fake_ctid);
        }
        return slot;
      }
    } else {
      ExecClearTuple(slot);
      FetchDataToVirtualTuple(fsstate, slot->tts_mcxt, slot_get_values(slot), slot_get_isnull(slot));
      ExecStoreVirtualTuple(slot);
      return slot;
    }
  }
  return ExecClearTuple(slot);
}

/*
 * Rescan table, possibly with new parameters
 */
static void TeaReScanForeignScan(ForeignScanState* node) {
  elog(DEBUG1, "! TeaReScanForeignScan");
  // TeaFdwExecutionState *festate = (TeaFdwExecutionState *)node->fdw_state;
}

/*
 * Finish scanning foreign table and dispose objects used for this scan
 */
static void TeaEndForeignScan(ForeignScanState* node) {
  elog(DEBUG1, "! TeaEndForeignScan");

  TeaScanState* fsstate = (TeaScanState*)node->fdw_state;
  // If festate is NULL, we are in EXPLAIN; nothing to do.
  if (fsstate == NULL) {
    return;
  }

  // Release remote connection.
  // MemoryContexts will be deleted automatically.
  if (fsstate->import.tea_ctx) {
    TeaContextLogStats(fsstate->import.tea_ctx, "FDW_QUERY");
    TeaContextDestroy(fsstate->import.tea_ctx);
  }

  fsstate->import.tea_ctx = NULL;
}

static bool FetchTupleNoClear(TeaScanState* fsstate, HeapTuple* rows, int pos) {
  HeapTuple tuple;
  /* Create a tuple from current result row. */
  if ((tuple = FetchData(fsstate, fsstate->batch_cxt))) {
    rows[pos] = tuple;
    return true;
  }
  return false;
}

/*
 * Acquire a random sample of rows from foreign table managed by postgres_fdw.
 *
 * We fetch the whole table from the remote side and pick out some sample rows.
 *
 * Selected rows are returned in the caller-allocated array rows[],
 * which must have at least targrows entries.
 * The actual number of rows selected is returned as the function result.
 * We also count the total number of rows in the table and return it into
 * *totalrows.  Note that *totaldeadrows is always set to 0.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the table.  Therefore, correlation estimates derived later
 * may be meaningless, but it's OK because we don't use the estimates
 * currently (the planner only pays attention to correlation for indexscans).
 */
static int TeaAcquireSampleRowsFunc(Relation relation, int elevel, HeapTuple* rows, int targrows, double* totalrows,
                                    double* totaldeadrows) {
  elog(DEBUG1, "! TeaAcquireSampleRowsFunc");

  TeaScanState* fsstate = (TeaScanState*)palloc0(sizeof(TeaScanState));
  int ncolumns;
  char session_id[SESSION_ID_LEN];

  fsstate->rel = relation;
  fsstate->location = TeaGetLocation(RelationGetRelid(relation));
  fsstate->attinmeta = TupleDescGetAttInMetadata(RelationGetDescr(fsstate->rel));

  ncolumns = fsstate->attinmeta->tupdesc->natts;

  Import* import = &fsstate->import;

  fsstate->is_remote_only = (bool*)palloc0(sizeof(bool) * ncolumns);
  import->ncolumns = ncolumns;
  import->columns = (int*)palloc0(sizeof(int) * ncolumns);
  for (int i = 0; i < ncolumns; ++i) {
    import->columns[i] = i;
  }

  fsstate->batch_cxt = CurrentMemoryContext;
  /* Create contexts for batches of tuples and per-tuple temp workspace. */
  fsstate->temp_cxt = AllocSetContextCreate(CurrentMemoryContext, "tea_fdw temporary data", ALLOCSET_SMALL_MINSIZE,
                                            ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE);

  import->values = palloc0(sizeof(Datum) * ncolumns);
  import->nulls = palloc0(sizeof(bool) * ncolumns);

  GetScanSessionId(session_id, SESSION_ID_LEN);
  memcpy(import->session_id, session_id, SESSION_ID_LEN);

  if (GetDatabaseEncoding() != PG_UTF8) {
    Oid conversion_proc = FindDefaultConversionProc(PG_UTF8, GetDatabaseEncoding());
    if (OidIsValid(conversion_proc)) {
      FmgrInfo* enc_conversion_proc = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
      fmgr_info(conversion_proc, enc_conversion_proc);
      fsstate->fmt_conversion_proc = enc_conversion_proc;
    }
  }

  const char* url = fsstate->location;

  import->tea_ctx = TeaContextCreate(url);
  AnalyzeParams* params = MakeAnalyzeParams(fsstate, url);

  TeaContextPlanAnalyze(import->tea_ctx, params);
  DestroyAnalyzeParams(params);

  /* First targrows rows are always included into the sample */
  bool has_row;
  int numrows = 0;
  double samplerows = 0.0;
  while (numrows < targrows) {
    CHECK_FOR_INTERRUPTS();

    TeaContextPrepareTuple(import->tea_ctx, &has_row);
    if (!has_row) {
      break;
    }

    if (FetchTupleNoClear(fsstate, rows, numrows)) {
      samplerows += 1;
      ++numrows;
    }
  }

  /*
   * Now we start replacing tuples in the sample until we reach the end of the relation.
   * It's the same algorithm as in acquire_sample_rows in analyze.c;
   * see Jeff Vitter's paper.
   */
  double rowstoskip = 0;
  double rstate = 0;
  while (has_row) {
    CHECK_FOR_INTERRUPTS();

    TeaContextPrepareTuple(import->tea_ctx, &has_row);
    if (!has_row) {
      break;
    }

    /*
     * Determine the slot where this sample row should be stored.  Set pos
     * to negative value to indicate the row should be skipped.
     */

    samplerows += 1;
    rowstoskip -= 1;
    if (rowstoskip < 0) {
      rowstoskip = anl_get_next_S(samplerows, targrows, &rstate);
    }
    const bool replace_element = (rowstoskip <= 0);

    if (replace_element) {
      /* Choose a random reservoir element to replace. */
      int pos = (int)(targrows * anl_random_fract());
      Assert(pos >= 0 && pos < targrows);
      heap_freetuple(rows[pos]);

      /* Create sample tuple from current result row. */
      if (!FetchTupleNoClear(fsstate, rows, pos)) {
        samplerows -= 1;
      }
    } else {
      TeaContextSkipTuple(import->tea_ctx);
    }
  }

  /*
   * Release remote connection.
   * MemoryContexts will be deleted automatically.
   */
  if (import->tea_ctx) {
    TeaContextLogStats(fsstate->import.tea_ctx, "FDW_SAMPLE");
    TeaContextDestroy(import->tea_ctx);
  }

  import->tea_ctx = NULL;

  /* We assume that we have no dead tuple. */
  *totaldeadrows = 0.0;

  /* We've retrieved all living tuples from foreign server. */
  *totalrows = samplerows;

  elog(LOG, "! TEA ANALYZE: total=%f num=%i", samplerows, numrows);

  return numrows;
}

/*
 * Test whether analyzing this foreign table is supported
 */
static bool TeaAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc* func, BlockNumber* totalpages) {
  elog(DEBUG1, "! TeaAnalyzeForeignTable");

  const char* url = TeaGetLocation(RelationGetRelid(relation));

  /* Return the row-analysis function pointer */
  *func = TeaAcquireSampleRowsFunc;

  /*
   * Now we have to get the number of pages.  It's annoying that the ANALYZE
   * API requires us to return that now, because it forces some duplication
   * of effort between this routine and postgresAcquireSampleRowsFunc.  But
   * it's probably not worth redefining that API at this point.
   */

  char session_id[SESSION_ID_LEN];

  GetScanSessionId(session_id, SESSION_ID_LEN);

  {
    TeaContextPtr tea_ctx = TeaContextCreate(url);
    TupleDesc desc = RelationGetDescr(relation);
    int ncolumns = desc->natts;
    bool* is_remote_only = (bool*)palloc0(sizeof(bool) * ncolumns);
    int* columns = (int*)palloc0(sizeof(int) * ncolumns);
    for (int i = 0; i < ncolumns; ++i) {
      columns[i] = i;
    }

    ReaderScanProjection projection = MakeScanProjection(desc, ncolumns, columns, is_remote_only);
    ReaderRelationSize relsize;
    TeaContextGetRelationSize(tea_ctx, session_id, &projection, &relsize);
    *totalpages = (relsize.rows * (relsize.width + sizeof(HeapTupleHeaderData))) / BLCKSZ;

    TeaContextLogStats(tea_ctx, "FDW_PLAN_ANALYZE");
    TeaContextDestroy(tea_ctx);
    pfree(is_remote_only);
    pfree(columns);
  }

  if (*totalpages == 0) {
    *totalpages = 1;
  }

  return true;
}

/*
 * Produce extra output for EXPLAIN
 */
static void TeaExplainForeignScan(ForeignScanState* node, ExplainState* es) {
  elog(DEBUG1, "! TeaExplainForeignScan");

  if (es->verbose) {
    List* fdw_private = ((ForeignScan*)node->ss.ps.plan)->fdw_private;
    const char* iceberg_metadata = strVal(list_nth(fdw_private, FdwScanPrivateMetadata));

    // seems too verbose
    if (iceberg_metadata) {
      ExplainPropertyText("Metadata", iceberg_metadata, es);
    }
  }
}

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum tea_fdw_handler(PG_FUNCTION_ARGS) {
  FdwRoutine* fdwroutine = makeNode(FdwRoutine);

  /* Support functions for ANALYZE */
  fdwroutine->AnalyzeForeignTable = TeaAnalyzeForeignTable;

  /* Support functions for EXPLAIN */
  fdwroutine->ExplainForeignScan = TeaExplainForeignScan;

  /* Functions for scanning foreign tables */
  fdwroutine->BeginForeignScan = TeaBeginForeignScan;
  fdwroutine->EndForeignScan = TeaEndForeignScan;
  fdwroutine->GetForeignPaths = TeaGetForeignPaths;
  fdwroutine->GetForeignPlan = TeaGetForeignPlan;
  fdwroutine->GetForeignRelSize = TeaGetForeignRelSize;
  fdwroutine->ReScanForeignScan = TeaReScanForeignScan;
  fdwroutine->IterateForeignScan = TeaIterateForeignScan;

  PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses tea_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum tea_fdw_validator(PG_FUNCTION_ARGS) { PG_RETURN_VOID(); }
