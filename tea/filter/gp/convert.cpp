#include "tea/filter/gp/convert.h"

#include "iceberg/filter/representation/visitor.h"

#include "tea/filter/gp/common.h"
#include "tea/filter/gp/serialized_filter.h"

extern "C" {
#include "postgres.h"  // NOLINT build/include_subdir

#include "access/tupdesc.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
}

#ifdef DAY
#undef DAY
#endif

#ifdef SECOND
#undef SECOND
#endif

#ifdef Abs
#undef Abs
#endif

#include <algorithm>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/serializer.h"
#include "iceberg/filter/representation/value.h"

#include "tea/filter/gp/array_converter.h"
#include "tea/filter/gp/pg_array.h"
#include "tea/filter/gp/types_mapping.h"
#include "tea/filter/gp/validate.h"
#include "tea/observability/tea_log.h"

namespace tea {
namespace {

using FNode = iceberg::filter::Node;
using FNodePtr = std::shared_ptr<FNode>;
using iceberg::filter::ConstNode;
using iceberg::filter::FunctionNode;
using iceberg::filter::ValueType;

class FilterConverter {
 public:
  explicit FilterConverter(TupleDesc tuple_desc, const IgnoredExprs& ignored_exprs)
      : tuple_desc_(tuple_desc), ignored_exprs_(ignored_exprs) {
    Oid conversion_proc = FindDefaultConversionProc(GetDatabaseEncoding(), PG_UTF8);
    if (OidIsValid(conversion_proc)) {
      enc_conversion_proc_ = std::make_unique<FmgrInfo>();
      fmgr_info(conversion_proc, enc_conversion_proc_.get());
    }
  }

  arrow::Result<FNodePtr> ConvertNode(Node* node) const;
  arrow::Result<FNodePtr> ConvertNodeTopLevel(Node* node) const;

 private:
  std::string ConvertString(const char* s, int len) const;
  iceberg::filter::Array<ValueType::kString> ConvertStringArray(const Datum* dats, bool* is_null, int len) const;
  std::string ConstNodeToString(Const* node) const;

  arrow::Result<FNodePtr> ConvertVar(Var* node) const;
  arrow::Result<std::shared_ptr<ConstNode>> ConvertConst(Const* node) const;
  arrow::Result<iceberg::filter::ArrayHolder> ConvertConstArray(Const* node) const;

  arrow::Result<FNodePtr> ConvertNullTest(NullTest* null) const;
  arrow::Result<FNodePtr> ConvertFuncExpr(FuncExpr* node) const;
  arrow::Result<FNodePtr> ConvertOpExpr(OpExpr* node) const;
  arrow::Result<FNodePtr> ConvertBoolExpr(BoolExpr* node) const;
  arrow::Result<FNodePtr> ConvertRelabelType(RelabelType* node) const;
  arrow::Result<FNodePtr> ConvertCaseExpr(CaseExpr* node) const;
  arrow::Result<FNodePtr> ConvertScalarArrayOpExpr(ScalarArrayOpExpr* node) const;
  arrow::Result<FNodePtr> ConvertBinaryBoolExpr(iceberg::filter::LogicalNode::Operation op, List* args) const;
  arrow::Result<FNodePtr> ConvertCoalesceExpr(CoalesceExpr* node) const;

  TupleDesc tuple_desc_ = nullptr;
  IgnoredExprs ignored_exprs_;
  std::unique_ptr<FmgrInfo> enc_conversion_proc_;
};

bool IsOidIgnored(const IntSpan& int_span, int value) {
  for (int i = 0; i < int_span.len; ++i) {
    if (int_span.data[i] == value) {
      return true;
    }
  }
  return false;
}

const char* AttNameFromAttNo(const TupleDesc tupdesc, int attno) { return NameStr(tupdesc->attrs[attno - 1]->attname); }

std::string FilterConverter::ConvertString(const char* s, int len) const {
  if (enc_conversion_proc_) {
    char* cvt = pg_server_to_custom(s, len, PG_UTF8, enc_conversion_proc_.get());
    if (cvt && cvt != s) {
      std::string result = std::string(cvt);
      pfree(cvt);
      return result;
    }
  }
  return std::string(s, len);
}

std::string FilterConverter::ConstNodeToString(Const* node) const {
  text* str_text = DatumGetTextP(node->constvalue);
  size_t len = VARSIZE(str_text) - VARHDRSZ;
  const char* s = text_to_cstring(str_text);
  return ConvertString(s, len);
}

std::string DatumToRawNumber(const Datum& datum) {
  Oid typoutput;
  bool typIsVarlena;
  char* extval;
  getTypeOutputInfo(NUMERICOID, &typoutput, &typIsVarlena);
  extval = OidOutputFunctionCall(typoutput, datum);
  std::string raw_number(extval);
  return raw_number;
}

arrow::Result<iceberg::filter::IntervalMonthMicro> ConstIntervalToMillis(const Datum& datum) {
  struct pg_tm tt, *tm = &tt;
  fsec_t fsec;

  if (interval2tm((*(Interval*)datum), tm, &fsec) != 0) {
    return arrow::Status::ExecutionError("Could not convert interval to tm");
  }

  int64_t months = tt.tm_year * 12 + tt.tm_mon;

  constexpr int64_t kMillisInSecond = 1000;
  constexpr int64_t kMicrosInMilli = 1000;
  constexpr int64_t kSecondsInMinute = 60;
  constexpr int64_t kMinutesInHour = 60;
  constexpr int64_t kHoursInDay = 24;
  constexpr int64_t kMicrosInSecond = kMicrosInMilli * kMillisInSecond;
  constexpr int64_t kMicrosInMinute = kMicrosInSecond * kSecondsInMinute;
  constexpr int64_t kMicrosInHour = kMicrosInMinute * kMinutesInHour;
  constexpr int64_t kMicrosInDay = kMicrosInHour * kHoursInDay;

  int64_t micros = tt.tm_sec * kMicrosInSecond + tt.tm_min * kMicrosInMinute + tt.tm_hour * kMicrosInMinute +
                   tt.tm_mday * kMicrosInDay;

  return iceberg::filter::IntervalMonthMicro{.months = months, .micros = micros};
}

arrow::Result<FNodePtr> FilterConverter::ConvertNodeTopLevel(Node* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertNodeTopLevel: node must not be nullptr");
  }
  NodeTag tag = nodeTag(node);
  if (tag != T_BoolExpr) {
    return ConvertNode(node);
  }
  BoolExpr* bool_expr_node = (BoolExpr*)(node);
  if (bool_expr_node->boolop != BoolExprType::AND_EXPR) {
    return ConvertNode(node);
  }

  ListCell* lc;
  std::vector<FNodePtr> processed_children;
  foreach (lc, bool_expr_node->args) {  // NOLINT whitespace/parens
    Node* child = reinterpret_cast<Node*> lfirst(lc);
    auto maybe_result = ConvertNodeTopLevel(child);
    if (maybe_result.ok()) {
      processed_children.emplace_back(maybe_result.MoveValueUnsafe());
    } else {
      TEA_LOG("Cannot convert clause: " + maybe_result.status().message());
    }
  }
  if (processed_children.empty()) {
    return arrow::Status::ExecutionError("ConvertNodeTopLevel: cannot convert any child");
  }
  if (processed_children.size() == 1) {
    return processed_children[0];
  }
  return std::make_shared<iceberg::filter::LogicalNode>(iceberg::filter::LogicalNode::Operation::kAnd,
                                                        std::move(processed_children));
}

arrow::Result<FNodePtr> FilterConverter::ConvertCoalesceExpr(CoalesceExpr* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertCoalesceExpr: node must not be nullptr");
  }
  ListCell* lc;
  std::vector<FNodePtr> processed_children;
  foreach (lc, node->args) {  // NOLINT whitespace/parens
    Node* child = reinterpret_cast<Node*> lfirst(lc);
    ARROW_ASSIGN_OR_RAISE(auto converted_child, ConvertNode(child));
    processed_children.emplace_back(converted_child);
  }

  if (processed_children.empty()) {
    return arrow::Status::ExecutionError("ConvertCoalesceExpr: cannot convert node with 0 children");
  }

  std::vector<ValueType> children_types;
  children_types.reserve(processed_children.size());
  for (const auto& child : processed_children) {
    iceberg::filter::TypeGetter type_getter;
    children_types.emplace_back(type_getter.Visit(child));
  }

  for (size_t i = 1; i < children_types.size(); ++i) {
    if (children_types[i] != children_types[i - 1]) {
      return arrow::Status::ExecutionError("ConvertCoalesceExpr: children types are not equal");
    }
  }

  const ValueType common_value_type = children_types[0];

  // input: (a, b, c)
  // output: isnull(a) ? (isnull(b) ? c : b) : a
  // we construct result as "c" -> "(isnull(b) ? c : b)" -> "isnull(a) ? (isnull(b) ? c : b) : a"
  FNodePtr result = processed_children.back();
  for (size_t position_from_end = 1; position_from_end < processed_children.size(); ++position_from_end) {
    const size_t position = processed_children.size() - position_from_end - 1;
    FNodePtr possible_value = processed_children.at(position);

    FNodePtr func = std::make_shared<FunctionNode>(
        iceberg::filter::FunctionSignature{iceberg::filter::FunctionID::kIsNull, ValueType::kBool, {common_value_type}},
        std::vector<FNodePtr>{possible_value});
    result = std::make_shared<iceberg::filter::IfNode>(func, result, possible_value);
  }

  return result;
}

arrow::Result<FNodePtr> FilterConverter::ConvertNode(Node* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertNode: node must not be nullptr");
  }
  NodeTag tag = nodeTag(node);
  switch (tag) {
    case T_Var:
      return ConvertVar((Var*)node);
    case T_Const:
      return ConvertConst((Const*)node);
    case T_FuncExpr:
      return ConvertFuncExpr((FuncExpr*)node);
    case T_NullTest:
      return ConvertNullTest((NullTest*)node);
    case T_OpExpr:
      return ConvertOpExpr((OpExpr*)node);
    case T_BoolExpr:
      return ConvertBoolExpr((BoolExpr*)node);
    case T_RelabelType:
      return ConvertRelabelType((RelabelType*)node);
    case T_CaseExpr:
      return ConvertCaseExpr((CaseExpr*)node);
    case T_ScalarArrayOpExpr:
      return ConvertScalarArrayOpExpr((ScalarArrayOpExpr*)node);
    case T_CoalesceExpr:
      return ConvertCoalesceExpr((CoalesceExpr*)node);
    default:
      return arrow::Status::NotImplemented("Node tag ", tag, " is not supported");
  }
}

arrow::Result<FNodePtr> FilterConverter::ConvertVar(Var* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertVar: node must not be nullptr");
  }
  if (node->varattno <= InvalidAttrNumber) {
    return arrow::Status::NotImplemented("ConvertVar: unsupported attribute number ", node->varattno);
  }

  auto maybe_var_type = tea::TypeOidToValueType(node->vartype);
  if (!maybe_var_type.has_value()) {
    return arrow::Status::NotImplemented("ConvertVar: unsupported arg type ", node->vartype);
  }
  iceberg::filter::ValueType value_type = maybe_var_type.value();

  std::string column_name = AttNameFromAttNo(tuple_desc_, node->varattno);
  return std::make_shared<iceberg::filter::VariableNode>(value_type, std::move(column_name));
}

arrow::Result<iceberg::filter::ArrayHolder> FilterConverter::ConvertConstArray(Const* node) const {
  if (node->constisnull || node->constbyval) {
    return arrow::Status::ExecutionError("ConvertConstArray: unexpected isnull ", node->constisnull, " byval ",
                                         node->constbyval);
  }
  ArrayType* arr = DatumGetArrayTypeP(node->constvalue);
  ARROW_ASSIGN_OR_RAISE(PGArray array, PGArray::Make(arr, node->consttype));

  return ArrayConverter::Convert(array, [this](const char* data, int len) { return ConvertString(data, len); });
}

arrow::Result<std::shared_ptr<ConstNode>> FilterConverter::ConvertConst(Const* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertConst: node must not be nullptr");
  }

  auto maybe_const_type = tea::TypeOidToValueType(node->consttype);
  if (!maybe_const_type.has_value()) {
    return arrow::Status::NotImplemented("ConvertConst: unsupported arg type ", node->consttype);
  }

  using iceberg::filter::Value;
  iceberg::filter::ValueType value_type = maybe_const_type.value();

  if (node->constisnull) {
    auto tag = DispatchTag(value_type);
    return std::visit(
        [&]<ValueType val_type>(iceberg::filter::Tag<val_type>) -> std::shared_ptr<ConstNode> {
          return std::make_shared<ConstNode>(Value::Make<val_type>());
        },
        tag);
  }

  switch (value_type) {
    case ValueType::kBool:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kBool>(DatumGetBool(node->constvalue)));
    case ValueType::kInt2:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kInt2>(DatumGetInt16(node->constvalue)));
    case ValueType::kInt4:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kInt4>(DatumGetInt32(node->constvalue)));
    case ValueType::kInt8:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kInt8>(DatumGetInt64(node->constvalue)));
    case ValueType::kFloat4:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kFloat4>(DatumGetFloat4(node->constvalue)));
    case ValueType::kFloat8:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kFloat8>(DatumGetFloat8(node->constvalue)));
    case ValueType::kNumeric:
      return std::make_shared<ConstNode>(
          Value::Make<ValueType::kNumeric>(iceberg::filter::Numeric{.value = DatumToRawNumber(node->constvalue)}));
    case ValueType::kString:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kString>(ConstNodeToString(node)));
    case ValueType::kDate:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kDate>(DatumGetUnixDate(node->constvalue)));
    case ValueType::kTimestamp:
      return std::make_shared<ConstNode>(
          Value::Make<ValueType::kTimestamp>(DatumGetUnixTimestampMicros(node->constvalue)));
    case ValueType::kTimestamptz:
      return std::make_shared<ConstNode>(
          Value::Make<ValueType::kTimestamptz>(DatumGetUnixTimestampMicros(node->constvalue)));
    case ValueType::kTime:
      return std::make_shared<ConstNode>(Value::Make<ValueType::kTime>(DatumGetTimeADT(node->constvalue)));
    case ValueType::kInterval: {
      ARROW_ASSIGN_OR_RAISE(auto millis, ConstIntervalToMillis(node->constvalue));
      return std::make_shared<ConstNode>(Value::Make<ValueType::kInterval>(millis));
    }
  }
  return arrow::Status::ExecutionError("Internal error in tea. ", __PRETTY_FUNCTION__);
}

arrow::Result<FNodePtr> FilterConverter::ConvertNullTest(NullTest* node) const {
  ARROW_ASSIGN_OR_RAISE(auto fnode, ConvertNode(reinterpret_cast<Node*>(node->arg)));
  iceberg::filter::TypeGetter type_getter;
  ValueType fnode_type = type_getter.Visit(fnode);

  FNodePtr func = std::make_shared<FunctionNode>(
      iceberg::filter::FunctionSignature{iceberg::filter::FunctionID::kIsNull, ValueType::kBool, {fnode_type}},
      std::vector<FNodePtr>{fnode});
  if (node->nulltesttype == IS_NOT_NULL) {
    func = std::make_shared<iceberg::filter::LogicalNode>(iceberg::filter::LogicalNode::Operation::kNot,
                                                          std::vector<FNodePtr>{func});
  }
  return func;
}

arrow::Result<FNodePtr> FilterConverter::ConvertFuncExpr(FuncExpr* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertFuncExpr: node must not be nullptr");
  }

  if (IsOidIgnored(ignored_exprs_.func_exprs, node->funcid)) {
    return arrow::Status::ExecutionError("ConvertFuncExpr: oid ", node->funcid, " is ignored");
  }
  auto maybe_func_type = tea::FuncOidToFunctionSignature(node->funcid);
  if (!maybe_func_type.has_value()) {
    return arrow::Status::NotImplemented("ConvertFuncExpr: unsupported function type ", node->funcid);
  }
  iceberg::filter::FunctionSignature function_signature = maybe_func_type.value();

  ListCell* lc;
  std::vector<Node*> arguments;
  foreach (lc, node->args) {  // NOLINT whitespace/parens
    arguments.push_back(reinterpret_cast<Node*>(lfirst(lc)));
  }
  std::vector<FNodePtr> processed_arguments;
  for (const auto& child_node : arguments) {  // NOLINT whitespace/parens
    ARROW_ASSIGN_OR_RAISE(auto result, ConvertNode(child_node));
    processed_arguments.emplace_back(std::move(result));
  }

  return std::make_shared<FunctionNode>(std::move(function_signature), std::move(processed_arguments));
}

arrow::Result<FNodePtr> FilterConverter::ConvertOpExpr(OpExpr* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertOpExpr: node must not be nullptr");
  }

  if (IsOidIgnored(ignored_exprs_.op_exprs, node->opno)) {
    return arrow::Status::ExecutionError("ConvertOpExpr: oid ", node->opno, " is ignored");
  }
  auto maybe_func_type = tea::OpExprOidToFunctionSignature(node->opno);
  if (!maybe_func_type.has_value()) {
    return arrow::Status::NotImplemented("ConvertOpExpr: unsupported operator type ", node->opno);
  }
  iceberg::filter::FunctionSignature function_signature = maybe_func_type.value();

  Node* leftop = get_leftop(reinterpret_cast<Expr*>(node));
  Node* rightop = get_rightop(reinterpret_cast<Expr*>(node));

  ARROW_ASSIGN_OR_RAISE(auto left_gnode, ConvertNode(leftop));
  if (!rightop) {  // unary operator
    return std::make_shared<FunctionNode>(std::move(function_signature), std::vector<FNodePtr>{left_gnode});
  }

  ARROW_ASSIGN_OR_RAISE(auto right_gnode, ConvertNode(rightop));
  return std::make_shared<FunctionNode>(std::move(function_signature), std::vector<FNodePtr>{left_gnode, right_gnode});
}

arrow::Result<FNodePtr> FilterConverter::ConvertBinaryBoolExpr(iceberg::filter::LogicalNode::Operation op,
                                                               List* args) const {
  if (args == nullptr) {
    return arrow::Status::ExecutionError("ConvertBinaryBoolExpr: args must not be nullptr");
  }

  ListCell* lc;
  std::vector<FNodePtr> processed_children;
  foreach (lc, args) {  // NOLINT whitespace/parens
    Node* node = reinterpret_cast<Node*> lfirst(lc);
    ARROW_ASSIGN_OR_RAISE(auto result, ConvertNode(node));
    processed_children.emplace_back(std::move(result));
  }
  return std::make_shared<iceberg::filter::LogicalNode>(op, std::move(processed_children));
}

arrow::Result<FNodePtr> FilterConverter::ConvertBoolExpr(BoolExpr* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertBoolExpr: node must not be nullptr");
  }

  switch (node->boolop) {
    case NOT_EXPR: {
      Node* arg = reinterpret_cast<Node*>(lfirst(list_head(node->args)));
      ARROW_ASSIGN_OR_RAISE(auto result, ConvertNode(arg));
      return std::make_shared<iceberg::filter::LogicalNode>(iceberg::filter::LogicalNode::Operation::kNot,
                                                            std::vector<FNodePtr>{result});
    }
    case AND_EXPR:
      return ConvertBinaryBoolExpr(iceberg::filter::LogicalNode::Operation::kAnd, node->args);
    case OR_EXPR:
      return ConvertBinaryBoolExpr(iceberg::filter::LogicalNode::Operation::kOr, node->args);
  }
  return arrow::Status::NotImplemented("BoolExpr: unsupported boolop ", node->boolop);
}

arrow::Result<FNodePtr> FilterConverter::ConvertRelabelType(RelabelType* node) const {
  return ConvertNode(reinterpret_cast<Node*>(node->arg));
}

arrow::Result<FNodePtr> FilterConverter::ConvertCaseExpr(CaseExpr* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ConvertCaseExpr: node must not be nullptr");
  }

  ListCell* lc;
  int args = 0;
  foreach (lc, node->args) {  // NOLINT whitespace/parens
    ++args;
    if (args >= 2) {
      return arrow::Status::NotImplemented("ConvertCaseExpr: expected 1 argument");
    }
  }
  CaseWhen* when_node = reinterpret_cast<CaseWhen*>(lfirst(list_head(node->args)));
  ARROW_ASSIGN_OR_RAISE(auto condition_node, ConvertNode(reinterpret_cast<Node*>(when_node->expr)));
  ARROW_ASSIGN_OR_RAISE(auto then_node, ConvertNode(reinterpret_cast<Node*>(when_node->result)));
  ARROW_ASSIGN_OR_RAISE(auto else_node, ConvertNode(reinterpret_cast<Node*>(node->defresult)));
  return std::make_shared<iceberg::filter::IfNode>(condition_node, then_node, else_node);
}

arrow::Result<FNodePtr> FilterConverter::ConvertScalarArrayOpExpr(ScalarArrayOpExpr* node) const {
  if (!node) {
    return arrow::Status::ExecutionError("ScalarArrayOpExpr: node must not be nullptr");
  }

  if (IsOidIgnored(ignored_exprs_.op_exprs, node->opno)) {
    return arrow::Status::ExecutionError("ConvertOpExpr: oid ", node->opno, " is ignored");
  }
  auto maybe_func_type = tea::OpExprOidToFunctionSignature(node->opno);
  if (!maybe_func_type.has_value()) {
    return arrow::Status::NotImplemented("ScalarArrayOpExpr: unsupported operator type ", node->opno);
  }
  iceberg::filter::FunctionSignature function_signature = maybe_func_type.value();

  Node* leftop = reinterpret_cast<Node*>(linitial(node->args));
  ARROW_ASSIGN_OR_RAISE(auto lhs, ConvertNode(leftop));

  Node* rightop = reinterpret_cast<Node*>(lsecond(node->args));
  if (!IsA(rightop, Const)) {
    return arrow::Status::NotImplemented("ConvertScalarArrayOpExpr: rightop must be const");
  }

  ARROW_ASSIGN_OR_RAISE(auto rhs, ConvertConstArray(reinterpret_cast<Const*>(rightop)));

  return std::make_shared<iceberg::filter::ScalarOverArrayFunctionNode>(std::move(function_signature), node->useOr, lhs,
                                                                        rhs);
}

class PGListModifier {
 public:
  // if list_to_modify is nullptr then this class does nothing
  explicit PGListModifier(List** list_to_modify) : list_(list_to_modify) {}

  void Append(void* datum) {
    if (list_) {
      *list_ = lappend(*list_, datum);
    }
  }

 private:
  List** list_;
};

struct ConvertResult {
  std::vector<FNodePtr> all_converted_nodes;  // (CAN be used for row-group and file filters)
  std::vector<FNodePtr> row_filter_nodes;     // (MUST be used for row filters on fdw)
};

ConvertResult ConvertPGClausesToTeaNodesVec(TeaTableType table_type, List* input_clauses, PGListModifier remote_clauses,
                                            PGListModifier local_clauses, TupleDesc tuple_desc,
                                            const IgnoredExprs& ignored_exprs) {
  tea::FilterConverter converter(tuple_desc, ignored_exprs);

  ConvertResult result;

  if (input_clauses) {
    ListCell* lc;
    foreach (lc, input_clauses) {  // NOLINT whitespace/parens
      Node* clause;
      RestrictInfo* ri;
      if (table_type == TeaTableType::kForeign) {
        ri = (RestrictInfo*)lfirst(lc);
        clause = reinterpret_cast<Node*>(ri->clause);
      } else {
        clause = reinterpret_cast<Node*>(lfirst(lc));
      }

      try {
        auto maybe_converted_clause =
            table_type == kExternal ? converter.ConvertNodeTopLevel(clause) : converter.ConvertNode(clause);
        if (maybe_converted_clause.ok()) {
          auto expr = maybe_converted_clause.ValueUnsafe();
          result.all_converted_nodes.emplace_back(expr);

          // validation is not required for external table
          auto status = table_type == kExternal ? arrow::Status::OK() : ValidateRowFilterClause(expr, tuple_desc);
          if (status.ok()) {
            result.row_filter_nodes.emplace_back(expr);
            remote_clauses.Append(lfirst(lc));
            continue;
          } else {
            TEA_LOG("Filter validation failed: " + status.message());
          }
        } else {
          TEA_LOG("Cannot convert clause: " + maybe_converted_clause.status().message());
        }
      } catch (const std::exception& e) {
        TEA_LOG(std::string("ConvertPGClausesToTeaNodesVec: unexpected error ") + e.what());
      } catch (const arrow::Status& e) {
        TEA_LOG(std::string("ConvertPGClausesToTeaNodesVec: unexpected error ") + e.message());
      } catch (...) {
        TEA_LOG("ConvertPGClausesToTeaNodesVec: unexpected unknown error");
      }
      local_clauses.Append(lfirst(lc));
    }
  }

  return result;
}

}  // namespace
}  // namespace tea

CSerializedFilter ConvertPGClausesToTeaNodes(TeaTableType table_type, List* input_clauses, List** remote_clauses,
                                             List** local_clauses, TupleDesc tuple_desc, IgnoredExprs ignored_exprs) {
  try {
    if (local_clauses) {
      *local_clauses = NIL;
    }
    if (remote_clauses) {
      *remote_clauses = NIL;
    }

    auto [all_nodes, row_filter_nodes] =
        ConvertPGClausesToTeaNodesVec(table_type, input_clauses, tea::PGListModifier(remote_clauses),
                                      tea::PGListModifier(local_clauses), tuple_desc, ignored_exprs);

    auto nodes_to_char = [](std::vector<tea::FNodePtr> nodes) -> char* {
      tea::FNodePtr result;
      if (nodes.empty()) {
        return nullptr;
      } else if (nodes.size() == 1) {
        result = nodes[0];
      } else {
        result = std::make_shared<iceberg::filter::LogicalNode>(iceberg::filter::LogicalNode::Operation::kAnd,
                                                                std::move(nodes));
      }
      std::string str = iceberg::filter::FilterToString(result);
      char* res = pstrdup(str.c_str());

      return res;
    };

    CSerializedFilter result{0};
    result.all_extracted = nodes_to_char(std::move(all_nodes));
    result.row = nodes_to_char(std::move(row_filter_nodes));
    return result;
  } catch (const std::exception& e) {
    elog(ERROR, "ConvertPGClausesToTeaNodesVec: internal error %s", e.what());
  } catch (const arrow::Status& e) {
    elog(ERROR, "ConvertPGClausesToTeaNodesVec: internal error %s", e.message().c_str());
  } catch (...) {
    elog(ERROR, "ConvertPGClausesToTeaNodes: internal error.");
  }
}
