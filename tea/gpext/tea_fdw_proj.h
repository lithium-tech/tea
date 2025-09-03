#ifndef TEA_FDW_H
#define TEA_FDW_H

#include "postgres.h"

#include "access/formatter.h"
#include "nodes/pg_list.h"
#include "utils/rel.h"

#if PG_VERSION_NUM >= 90600
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif

/*
 * We create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
extern void DeparseTargetList(const Relation rel, const Bitmapset *attrs_used, List **retrieved_attrs);

/*
 * Identify which attributes will need to be retrieved from the remote
 * server.  These include all attrs needed for joins or final output, plus
 * all attrs used in the local_conds.  (Note: if we end up using a
 * parameterized scan, it's possible that some of the join clauses will be
 * sent to the remote and thus we wouldn't really need to retrieve the
 * columns used in them.  Doesn't seem worth detecting that case though.)
 */
extern Bitmapset *GetUsedAttributesSet(RelOptInfo *baserel, List *local_conds);

#endif  // TEA_FDW_H
