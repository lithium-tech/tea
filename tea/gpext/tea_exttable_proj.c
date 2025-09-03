#include "tea/gpext/tea_exttable_proj.h"

#include "access/fileam.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/walkers.h"

static int *GetVarNumbers(ProjectionInfo *projInfo);
static List *GetTargetList(ProjectionInfo *projInfo);
static bool NeedToIterateTargetList(List *targetList, int *varNumbers);
static Node *GetTargetListEntryExpression(ListCell *lc1);
static bool AddAttnumsFromTargetList(Node *node, List *attnums);
static bool GetVarnoFromTargetList(Node *node, Index **varno);
static bool GetVarnoFromQuals(Node *node, Index **varno);
static int GetNumSimpleVars(ProjectionInfo *projInfo);
static List *GetAttrsFromQualsWithVarno(List *quals, Index varno);

static List *GetAttrsFromQuals(List *quals, bool *subtree_is_supported);
static Bitmapset *GetUsedAttrsSet(ExternalSelectDesc desc, bool *full_row, bool ext_table_filter_walker_for_projection);

int GetRequiredColumns(ExternalSelectDesc desc, int *columns, int ncolumns,
                       bool ext_table_filter_walker_for_projection) {
  int *out = columns;
  bool full_row = false;
  Bitmapset *attrs_used = GetUsedAttrsSet(desc, &full_row, ext_table_filter_walker_for_projection);
  if (full_row) {
    for (int i = 0; i < ncolumns; ++i) {
      *out++ = i;
    }
  } else if (attrs_used) {
    for (int i = 0; i < ncolumns; ++i) {
      if (bms_is_member(i, attrs_used)) {
        *out++ = i;
      }
    }
    bms_free(attrs_used);
  } else {
    // return no columns
  }
  return out - columns;
}

static Bitmapset *GetUsedAttrsSet(ExternalSelectDesc desc, bool *full_row,
                                  bool ext_table_filter_walker_for_projection) {
  Bitmapset *attrs_used = NULL;  // hashset to keep and de-dup collected attrnos

  if (!desc->projInfo) {
    *full_row = true;
    return NULL;
  }

  List *target_list = GetTargetList(desc->projInfo);
  int *var_numbers = GetVarNumbers(desc->projInfo);
  // STEP 1: collect attribute numbers (attrno) from the targetList, if
  // necessary
  if (NeedToIterateTargetList(target_list, var_numbers)) {
    /*
     * we use expression_tree_walker to access attrno information
     * we do it through a helper function add_attnums_from_targetList
     */
    List *l = lappend_int(NIL, 0);
    ListCell *lc1;
    foreach (lc1, target_list) {
      if (IsA(lfirst(lc1), GenericExprState)) {
        Node *node = GetTargetListEntryExpression(lc1);
        AddAttnumsFromTargetList(node, l);
      } else {
        elog(LOG, "GetUsedAttrsSet: unexpected node type %d", nodeTag(lfirst(lc1)));
        *full_row = true;
        list_free(l);
        return NULL;
      }
    }
    foreach (lc1, l) {
      int attno = lfirst_int(lc1);
      if (attno > InvalidAttrNumber) {
        attrs_used = bms_add_member(attrs_used, attno - 1);
      }
    }
    list_free(l);
  }

  // STEP 2: collect attribute numbers from pre-computed list of varNumbers
  // (if available) of simpleVars
  if (var_numbers) {
    int numSimpleVars = GetNumSimpleVars(desc->projInfo);
    for (int i = 0; i < numSimpleVars; i++) {
      attrs_used = bms_add_member(attrs_used, var_numbers[i] - 1);
    }
  }

  // STEP 3: collect attribute numbers from qualifiers (WHERE conditions)
  List *quals_attributes = NULL;
  if (desc->filter_quals) {
    /* projection information is incomplete if columns from WHERE clause wasn't
     * extracted */
    bool quals_are_supported = true;
    quals_attributes = GetAttrsFromQuals(desc->filter_quals, &quals_are_supported);
    if (!quals_are_supported) {
      if (ext_table_filter_walker_for_projection) {
        // fallback: try to get varno of current table and recursively pull all varattnos with this varno on current
        // level
        Index *varno = NULL;
        ListCell *lc1;
        foreach (lc1, target_list) {
          Node *node = GetTargetListEntryExpression(lc1);
          GetVarnoFromTargetList((Node *)node, &varno);
        }
        GetVarnoFromQuals((Node *)desc->filter_quals, &varno);

        if (varno != NULL) {
          quals_attributes = GetAttrsFromQualsWithVarno(desc->filter_quals, *varno);
        } else {
          *full_row = true;
        }
      } else {
        *full_row = true;
      }

      if (*full_row == true) {
        // cannot determine columns to return, so retrieve all of them
        return NULL;
      }
    }
  }

  ListCell *attribute = NULL;
  foreach (attribute, quals_attributes) {
    AttrNumber attrNumber = (AttrNumber)lfirst_int(attribute);
    attrs_used = bms_add_member(attrs_used, attrNumber);
  }
  return attrs_used;
}

/*
 * Returns pre-computed array of simple var attrnos, if available
 */
static inline int *GetVarNumbers(ProjectionInfo *projInfo) {
#if PG_VERSION_NUM >= 120000
  return NULL;  // does not exist in projInfo in GP7
#else
  return projInfo->pi_varNumbers;
#endif
}

/*
 * Returns targetList from provided ProjectionInfo
 */
static inline List *GetTargetList(ProjectionInfo *projInfo) {
#if PG_VERSION_NUM >= 120000
  return (List *)projInfo->pi_state.expr;
#else
  return projInfo->pi_targetlist;
#endif
}

/*
 * Determines whether there is a need to iterate over the targetList to find
 * projected attributes
 */
static inline bool NeedToIterateTargetList(List *targetList, int *varNumbers) {
#if PG_VERSION_NUM >= 90400
  /*
   * In GP6 non-simple Vars are added to the targetlist of ProjectionInfo
   * while simple Vars are pre-computed and their attnos are placed into
   * varNumbers array In GP7 everything is in targetList
   */
  return (targetList != NULL);
#else
  /*
   * In GP5 if targetList contains ONLY simple Vars their attrnos will be
   * populated into varNumbers array otherwise the varNumbers array will be
   * NULL, and we will need to iterate over the targetList
   */
  return (varNumbers == NULL);
#endif
}

/*
 * Returns expression for a targetList entry.
 */
static inline Node *GetTargetListEntryExpression(ListCell *lc1) {
#if PG_VERSION_NUM >= 120000
  ExprState *gstate = (ExprState *)lfirst(lc1);
  return (Node *)gstate;
#else
  GenericExprState *gstate = (GenericExprState *)lfirst(lc1);
  return (Node *)gstate->arg->expr;
#endif
}

/*
 * Gets a list of attnums from the given Node
 * it uses expression_tree_walker to recursively
 * get the list
 */
static bool AddAttnumsFromTargetList(Node *node, List *attnums) {
  if (node == NULL) return false;
  if (IsA(node, Var)) {
    Var *variable = (Var *)node;
    AttrNumber attnum = variable->varattno;

    lappend_int(attnums, attnum);
    return false;
  }

  /*
   * Don't examine the arguments or filters of Aggrefs or WindowFunc/WindowRef,
   * because those do not represent expressions to be evaluated within the
   * overall targetlist's econtext.
   */
  if (IsA(node, Aggref)) return false;
#if PG_VERSION_NUM >= 90400
  if (IsA(node, WindowFunc))
#else
  if (IsA(node, WindowRef))
#endif
    return false;
  return expression_tree_walker(node, AddAttnumsFromTargetList, (void *)attnums);
}

/*
 * Returns a count of simpleVars if they were pre-computed.
 */
static inline int GetNumSimpleVars(ProjectionInfo *projInfo) {
  int numSimpleVars = 0;
#if PG_VERSION_NUM < 90400
  // in GP5 if varNumbers is not NULL, it means the attrnos have been
  // pre-computed in varNumbers and targetList consists only of simpleVars, so
  // we can use its length
  if (projInfo->pi_varNumbers) {
    numSimpleVars = list_length(projInfo->pi_targetlist);
  }
#elif PG_VERSION_NUM < 120000
  // in GP6 we can get this value from projInfo
  numSimpleVars = projInfo->pi_numSimpleVars;
#else
  // in GP7 there is no precomputation, the numSimpleVars stays at 0
#endif
  return numSimpleVars;
}

static List *GetAttrsFromNode(Node *node, bool *subtree_is_supported);

static List *GetAttrsFromVar(Var *var_node, bool *subtree_is_supported) {
  AttrNumber varattno = var_node->varattno;
  /* system attr not supported */
  if (varattno <= InvalidAttrNumber) {
    *subtree_is_supported = false;
    return NULL;
  }

  List *result = NULL;
  return lappend_int(result, varattno - 1);
}

static List *GetAttrsFromConst(Const *const_node, bool *subtree_is_supported) { return NULL; }

static List *GetAttrsFromFuncExpr(FuncExpr *funcexpr_node, bool *subtree_is_supported) {
  List *result = NULL;

  ListCell *lc = NULL;
  foreach (lc, funcexpr_node->args) {
    Node *node = (Node *)lfirst(lc);
    List *new_attributes = GetAttrsFromNode(node, subtree_is_supported);
    result = list_concat(result, new_attributes);
  }
  return result;
}

static List *GetAttrsFromOpExpr(OpExpr *opexpr_node, bool *subtree_is_supported) {
  Node *leftop = get_leftop((Expr *)opexpr_node);
  Node *rightop = get_rightop((Expr *)opexpr_node);

  List *left_part = GetAttrsFromNode(leftop, subtree_is_supported);
  if (rightop) {
    List *right_part = GetAttrsFromNode(rightop, subtree_is_supported);
    return list_concat(left_part, right_part);
  }
  return left_part;
}

static List *GetAttrsFromBoolExpr(BoolExpr *boolexpr_node, bool *subtree_is_supported) {
  return GetAttrsFromQuals(boolexpr_node->args, subtree_is_supported);
}

static List *GetAttrsFromRelabelType(RelabelType *relabeltype_node, bool *subtree_is_supported) {
  return GetAttrsFromNode((Node *)relabeltype_node->arg, subtree_is_supported);
}

static List *GetAttrsFromNullTest(NullTest *nulltest_node, bool *subtree_is_supported) {
  return GetAttrsFromNode((Node *)nulltest_node->arg, subtree_is_supported);
}

static List *GetAttrsFromScalarArrayOpExpr(ScalarArrayOpExpr *scalararrayopexpr_node, bool *subtree_is_supported) {
  Node *leftop = (Node *)linitial(scalararrayopexpr_node->args);
  Node *rightop = (Node *)lsecond(scalararrayopexpr_node->args);

  List *left_part = GetAttrsFromNode(leftop, subtree_is_supported);
  List *right_part = GetAttrsFromNode(rightop, subtree_is_supported);
  return list_concat(left_part, right_part);
}

static List *GetAttrsFromCoalesceExpr(CoalesceExpr *coalesceexpr_node, bool *subtree_is_supported) {
  return GetAttrsFromQuals(coalesceexpr_node->args, subtree_is_supported);
}

static List *GetAttrsFromCaseExpr(CaseExpr *caseexpr_node, bool *subtree_is_supported) {
  int args_count = list_length(caseexpr_node->args);
  if (args_count != 1) {
    elog(LOG, "Unexpected number of arguments in case when: %d", args_count);
    *subtree_is_supported = false;
    return NULL;
  }
  CaseWhen *when_node = (CaseWhen *)(lfirst(list_head(caseexpr_node->args)));
  List *if_part = GetAttrsFromNode((Node *)when_node->expr, subtree_is_supported);
  List *then_part = GetAttrsFromNode((Node *)when_node->result, subtree_is_supported);
  List *else_part = GetAttrsFromNode((Node *)when_node->result, subtree_is_supported);
  return list_concat(if_part, list_concat(then_part, else_part));
}

static List *GetAttrsFromNode(Node *node, bool *subtree_is_supported) {
  if (!(*subtree_is_supported)) {
    return NULL;
  }
  if (!node) {
    elog(LOG, "Unexpected nullptr node");
    *subtree_is_supported = false;
    return NULL;
  }
  NodeTag tag = nodeTag(node);
  switch (tag) {
    case T_Var:
      return GetAttrsFromVar((Var *)node, subtree_is_supported);
    case T_Const:
      return GetAttrsFromConst((Const *)node, subtree_is_supported);
    case T_FuncExpr:
      return GetAttrsFromFuncExpr((FuncExpr *)node, subtree_is_supported);
    case T_OpExpr:
      return GetAttrsFromOpExpr((OpExpr *)node, subtree_is_supported);
    case T_BoolExpr:
      return GetAttrsFromBoolExpr((BoolExpr *)node, subtree_is_supported);
    case T_RelabelType:
      return GetAttrsFromRelabelType((RelabelType *)node, subtree_is_supported);
    case T_NullTest:
      return GetAttrsFromNullTest((NullTest *)node, subtree_is_supported);
    case T_CaseExpr:
      return GetAttrsFromCaseExpr((CaseExpr *)node, subtree_is_supported);
    case T_ScalarArrayOpExpr:
      return GetAttrsFromScalarArrayOpExpr((ScalarArrayOpExpr *)node, subtree_is_supported);
    case T_CoalesceExpr:
      return GetAttrsFromCoalesceExpr((CoalesceExpr *)node, subtree_is_supported);
    default:
      *subtree_is_supported = false;
      elog(LOG, "Selectdesc: node tag %d is not supported", tag);
      return NULL;
  }

  *subtree_is_supported = false;
  return NULL;
}

static List *GetAttrsFromQuals(List *quals, bool *subtree_is_supported) {
  ListCell *lc = NULL;
  List *attributes = NIL;

  if (list_length(quals) == 0) return NIL;

  foreach (lc, quals) {
    Node *node = (Node *)lfirst(lc);
    List *new_attributes = GetAttrsFromNode(node, subtree_is_supported);
    attributes = list_concat(attributes, new_attributes);
  }

  return attributes;
}

static bool GetVarnoFromQuals(Node *node, Index **varno) {
  if (node == NULL) return false;
  NodeTag node_tag = nodeTag(node);
  switch (node_tag) {
    case T_Var: {
      Var *variable = (Var *)node;
      *varno = &variable->varno;
      return true;
    }
    case T_Const:
    case T_FuncExpr:
    case T_OpExpr:
    case T_BoolExpr:
    case T_RelabelType:
    case T_NullTest:
    case T_CaseExpr:
    case T_ScalarArrayOpExpr:
    case T_List:
      return expression_tree_walker(node, GetVarnoFromQuals, (void *)varno);
    default:
      return false;
  }
}

static bool GetVarnoFromTargetList(Node *node, Index **varno) {
  if (node == NULL) return false;
  if (IsA(node, Var)) {
    Var *variable = (Var *)node;
    *varno = &variable->varno;
    return true;
  }

  /*
   * Don't examine the arguments or filters of Aggrefs or WindowFunc/WindowRef,
   * because those do not represent expressions to be evaluated within the
   * overall targetlist's econtext.
   */
  if (IsA(node, Aggref)) return false;
#if PG_VERSION_NUM >= 90400
  if (IsA(node, WindowFunc))
#else
  if (IsA(node, WindowRef))
#endif
    return false;
  return expression_tree_walker(node, GetVarnoFromTargetList, (void *)varno);
}

typedef struct {
  List *varattnos;
  Index varno;
} PullVarattnosContext;

static bool PullVarattnosWalker(Node *node, PullVarattnosContext *context) {
  if (node == NULL) return false;
  if (IsA(node, Var)) {
    Var *var = (Var *)node;
    if (var->varattno <= InvalidAttrNumber) {
      return false;
    }
    if (var->varno == context->varno && var->varlevelsup == 0)
      context->varattnos = lappend_int(context->varattnos, var->varattno - 1);
    return false;
  }

  /* Should not find an unplanned subquery */
  Assert(!IsA(node, Query));

  return expression_tree_walker(node, PullVarattnosWalker, (void *)context);
}

static List *GetAttrsFromQualsWithVarno(List *quals, Index varno) {
  List *attributes = NULL;

  if (list_length(quals) == 0) return NULL;

  PullVarattnosContext context;

  context.varattnos = attributes;
  context.varno = varno;

  (void)PullVarattnosWalker((Node *)quals, &context);

  attributes = context.varattnos;

  return attributes;
}
