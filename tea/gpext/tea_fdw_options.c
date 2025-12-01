#include "tea/gpext/tea_fdw_options.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "storage/lock.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct TeaOption {
  const char* optname;
  /// Oid of catalog in which option may appear.
  Oid optcontext;
};

/*
 * Retrieve per-column generic options from pg_attribute and construct a list
 * of DefElems representing them.
 *
 * At the moment we only have "force_not_null", and "force_null",
 * which should each be combined into a single DefElem listing all such
 * columns, since that's what COPY expects.
 */
static List* get_tea_fdw_attribute_options(Oid relid) {
  Relation rel;
  TupleDesc tupleDesc;
  AttrNumber natts;
  AttrNumber attnum;
  List* fnncolumns = NIL;
  List* fncolumns = NIL;

  List* options = NIL;

  rel = heap_open(relid, AccessShareLock);
  tupleDesc = RelationGetDescr(rel);
  natts = tupleDesc->natts;

  /* Retrieve FDW options for all user-defined attributes. */
  for (attnum = 1; attnum <= natts; attnum++) {
    Form_pg_attribute attr = tupleDesc->attrs[attnum - 1];
    List* fc_options;
    ListCell* lc;

    /* Skip dropped attributes. */
    if (attr->attisdropped) continue;

    fc_options = GetForeignColumnOptions(relid, attnum);
    foreach (lc, fc_options) {
      DefElem* def = (DefElem*)lfirst(lc);

      if (strcmp(def->defname, "force_not_null") == 0) {
        if (defGetBoolean(def)) {
          char* attname = pstrdup(NameStr(attr->attname));

          fnncolumns = lappend(fnncolumns, makeString(attname));
        }
      } else if (strcmp(def->defname, "force_null") == 0) {
        if (defGetBoolean(def)) {
          char* attname = pstrdup(NameStr(attr->attname));

          fncolumns = lappend(fncolumns, makeString(attname));
        }
      }
      /* maybe in future handle other options here */
    }
  }

  heap_close(rel, AccessShareLock);

  /*
   * Return DefElem only when some column(s) have force_not_null /
   * force_null options set
   */
  if (fnncolumns != NIL) options = lappend(options, makeDefElem("force_not_null", (Node*)fnncolumns));

  if (fncolumns != NIL) options = lappend(options, makeDefElem("force_null", (Node*)fncolumns));

  return options;
}

char* TeaGetLocation(Oid foreigntableid) {
  ForeignTable* table;
  ForeignServer* server;
  ForeignDataWrapper* wrapper;
  List* options;
  ListCell* lc;

  /*
   * Extract options from FDW objects.  We ignore user mappings because
   * tea_fdw doesn't have any options that can be specified there.
   */
  table = GetForeignTable(foreigntableid);
  server = GetForeignServer(table->serverid);
  wrapper = GetForeignDataWrapper(server->fdwid);

  options = NIL;
  options = list_concat(options, wrapper->options);
  options = list_concat(options, server->options);
  options = list_concat(options, table->options);
  options = list_concat(options, get_tea_fdw_attribute_options(foreigntableid));

  foreach (lc, options) {
    DefElem* def = (DefElem*)lfirst(lc);

    if (strcmp(def->defname, "location") == 0) {
      return defGetString(def);
    }
  }
  return NULL;
}
