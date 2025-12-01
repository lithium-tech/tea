/*-------------------------------------------------------------------------
 *
 * This file includes functions that examine query WHERE clauses to see
 * whether they're safe to send to the remote server for execution.
 *
 *-------------------------------------------------------------------------
 */
#include "tea/gpext/tea_fdw_proj.h"

#include "postgres.h"

#include "optimizer/var.h"

void DeparseTargetList(const Relation rel, const Bitmapset* attrs_used, List** retrieved_attrs) {
  *retrieved_attrs = NIL;

  // If there's a whole-row reference, we'll need all the columns.
  const bool have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

  const TupleDesc tupdesc = RelationGetDescr(rel);
  for (int i = 1; i <= tupdesc->natts; i++) {
    // Ignore dropped attributes.
    if (tupdesc->attrs[i - 1]->attisdropped) {
      continue;
    }

    if (have_wholerow || bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used)) {
      *retrieved_attrs = lappend_int(*retrieved_attrs, i);
    }
  }
}

Bitmapset* GetUsedAttributesSet(RelOptInfo* baserel, List* local_conds) {
  Bitmapset* attrs_used = NULL;
  ListCell* lc;

#if (PG_VERSION_NUM < 90000)
  pull_varattnos((Node*)baserel, &attrs_used);
#elif (PG_VERSION_NUM <= 90500)
  pull_varattnos((Node*)baserel->reltargetlist, baserel->relid, &attrs_used);
#else
  pull_varattnos((Node*)baserel->reltarget->exprs, baserel->relid, &attrs_used);
#endif

  foreach (lc, local_conds) {
    RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

#if (PG_VERSION_NUM < 90000)
    pull_varattnos((Node*)rinfo, &attrs_used);
#else
    pull_varattnos((Node*)rinfo->clause, baserel->relid, &attrs_used);
#endif
  }

  return attrs_used;
}
