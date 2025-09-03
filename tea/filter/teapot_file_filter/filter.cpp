#include "tea/filter/teapot_file_filter/filter.h"

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/value.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "tea/filter/teapot_file_filter/node.h"
#include "tea/filter/teapot_file_filter/serializer.h"

namespace tea::filter {

namespace {

using Context = TeapotFileFilterContext;

using NodeType = iceberg::filter::NodeType;
using NodePtr = iceberg::filter::NodePtr;
using ValueType = iceberg::filter::ValueType;
using FunctionID = iceberg::filter::FunctionID;
using FNode = file_filter::Node;
using FNodePtr = std::shared_ptr<FNode>;
using Operator = file_filter::Operator;

arrow::Result<FNodePtr> ProcessRoot(NodePtr node, const Context& ctx);

arrow::Result<Operator> FunctionIDToOperator(FunctionID id) {
  switch (id) {
    case FunctionID::kLessThan:
      return Operator::kLt;
    case FunctionID::kLessThanOrEqualTo:
      return Operator::kLtEq;
    case FunctionID::kGreaterThan:
      return Operator::kGt;
    case FunctionID::kGreaterThanOrEqualTo:
      return Operator::kGtEq;
    case FunctionID::kEqual:
      return Operator::kEq;
    case FunctionID::kNotEqual:
      return Operator::kNotEq;
    case FunctionID::kLike:
      return Operator::kStartsWith;
    case FunctionID::kNotLike:
      return Operator::kNotStartsWith;
    default:
      return arrow::Status::ExecutionError("FunctionIDToOperator: unexpected id ", static_cast<int>(id));
  }
}

arrow::Result<Operator> FlipOperator(Operator op) {
  switch (op) {
    case Operator::kLt:
      return Operator::kGt;
    case Operator::kLtEq:
      return Operator::kGtEq;
    case Operator::kGt:
      return Operator::kLt;
    case Operator::kGtEq:
      return Operator::kLtEq;
    case Operator::kEq:
      return Operator::kEq;
    case Operator::kNotEq:
      return Operator::kNotEq;
    case Operator::kAnd:
      return Operator::kAnd;
    case Operator::kOr:
      return Operator::kOr;
    default:
      return arrow::Status::ExecutionError("Cannot flip operator ", static_cast<int>(op));
  }
}

arrow::Result<FNodePtr> ProcessVariable(std::shared_ptr<iceberg::filter::VariableNode> node) {
  if (node->value_type != ValueType::kBool) {
    return arrow::Status::ExecutionError("ProcessVar: unexpected type ", static_cast<int>(node->value_type));
  }
  return std::make_shared<file_filter::ComparisonNode>(Operator::kEq, node->column_name, true);
}

arrow::Result<file_filter::ComparisonNode::ValueHolder> ProcessConst(std::shared_ptr<iceberg::filter::ConstNode> node) {
  if (node->value.IsNull()) {
    return arrow::Status::ExecutionError("ProcessConst: expected non-null value");
  }
  switch (node->value.GetValueType()) {
    case ValueType::kBool:
      return node->value.GetValue<ValueType::kBool>();
    case ValueType::kInt2:
      return node->value.GetValue<ValueType::kInt2>();
    case ValueType::kInt4:
      return node->value.GetValue<ValueType::kInt4>();
    case ValueType::kInt8:
      return node->value.GetValue<ValueType::kInt8>();
    case ValueType::kFloat4:
      return node->value.GetValue<ValueType::kFloat4>();
    case ValueType::kFloat8:
      return node->value.GetValue<ValueType::kFloat8>();
    case ValueType::kDate:
      return node->value.GetValue<ValueType::kDate>();
    case ValueType::kTimestamp:
      return node->value.GetValue<ValueType::kTimestamp>();
    case ValueType::kTimestamptz:
      return node->value.GetValue<ValueType::kTimestamptz>();
    case ValueType::kNumeric:
      return node->value.GetValue<ValueType::kNumeric>().value;
    case ValueType::kString:
      return node->value.GetValue<ValueType::kString>();
    case ValueType::kTime:
      return node->value.GetValue<ValueType::kTime>();
    default:
      return arrow::Status::ExecutionError("ProcessConst: unexpected type ",
                                           static_cast<int>(node->value.GetValueType()));
  }
  return arrow::Status::ExecutionError(__PRETTY_FUNCTION__);
}

constexpr int64_t kSecondsPerDay = 60 * 60 * 24;
constexpr int64_t kMillisecondsPerDay = kSecondsPerDay * 1000;
constexpr int64_t kMicrosecondsPerDay = kMillisecondsPerDay * 1000;

arrow::Status TryToCastTimestamp(int64_t& value, ValueType to_type, const Context& ctx) {
  if (to_type == ValueType::kTimestamptz) {
    value += ctx.timestamp_to_timestamptz_shift_us_;
  } else if (to_type == ValueType::kDate) {
    if (value % kMicrosecondsPerDay != 0) {
      return arrow::Status::ExecutionError("Cannot cast timestamp to date");
    }
    value /= kMicrosecondsPerDay;
  }
  return arrow::Status::OK();
}

arrow::Status TryToCastTimestamptz(int64_t& value, ValueType to_type, const Context& ctx) {
  if (to_type == ValueType::kTimestamp) {
    value -= ctx.timestamp_to_timestamptz_shift_us_;
  } else if (to_type == ValueType::kDate) {
    value -= ctx.timestamp_to_timestamptz_shift_us_;
    if (value % kMicrosecondsPerDay != 0) {
      return arrow::Status::ExecutionError("Cannot cast timestamptz to date");
    }
    value /= kMicrosecondsPerDay;
  }
  return arrow::Status::OK();
}

arrow::Status TryToCastDate(int64_t& value, ValueType to_type, const Context& ctx) {
  if (to_type == ValueType::kTimestamp) {
    value *= kMicrosecondsPerDay;
  } else if (to_type == ValueType::kTimestamptz) {
    value *= kMicrosecondsPerDay;
    value += ctx.timestamp_to_timestamptz_shift_us_;
  }
  return arrow::Status::OK();
}

arrow::Status TryToCastIntValue(int64_t& value, ValueType from_type, ValueType to_type, const Context& ctx) {
  switch (from_type) {
    case ValueType::kTimestamp:
      return TryToCastTimestamp(value, to_type, ctx);
    case ValueType::kTimestamptz:
      return TryToCastTimestamptz(value, to_type, ctx);
    case ValueType::kDate:
      return TryToCastDate(value, to_type, ctx);
    default:
      return arrow::Status::OK();
  }
}

arrow::Status TryToCastIntValueArray(std::vector<int64_t>& values, ValueType from_type, ValueType to_type,
                                     const Context& ctx) {
  switch (from_type) {
    case ValueType::kTimestamp:
      for (int64_t& value : values) {
        ARROW_RETURN_NOT_OK(TryToCastTimestamp(value, to_type, ctx));
      }
      break;
    case ValueType::kTimestamptz:
      for (int64_t& value : values) {
        ARROW_RETURN_NOT_OK(TryToCastTimestamptz(value, to_type, ctx));
      }
      break;
    case ValueType::kDate:
      for (int64_t& value : values) {
        ARROW_RETURN_NOT_OK(TryToCastDate(value, to_type, ctx));
      }
      break;
    default:
      return arrow::Status::OK();
  }
  return arrow::Status::OK();
}

arrow::Result<std::string> GetStartsWithStringFromLikeString(const std::string& s) {
  if (s.empty()) {
    return arrow::Status::ExecutionError("GetStartsWithString: empty string is unexpected");
  }
  std::string result;
  for (size_t i = 0; i < s.size(); ++i) {
    if (s[i] == '%') {
      if (i + 1 == s.size()) {
        return result;
      }
      return arrow::Status::ExecutionError("GetStartsWithString: symbol '%' in the middle of string is unexpected");
    }

    if (s[i] == '_') {
      return arrow::Status::ExecutionError("GetStartsWithString: symbol '_' is unexpected");
    }

    if (s[i] == '\\') {
      if (i + 1 < s.size() && (s[i + 1] == '\\' || s[i + 1] == '_' || s[i + 1] == '%')) {
        result += s[i + 1];
        ++i;
        continue;
      } else {
        if (i + 1 == s.size()) {
          return arrow::Status::ExecutionError("GetStartsWithString: string with symbol '", s.back(),
                                               "' in the end is unexpected");
        } else {
          return arrow::Status::ExecutionError("GetStartsWithString: escaping symbol '", s[i + 1], "' is unexpected'");
        }
      }
    }

    result += s[i];
  }

  return arrow::Status::ExecutionError("GetStartsWithString: string with symbol '", s.back(),
                                       "' in the end is unexpected");
}

template <typename ToType, ValueType value_type>
arrow::Result<file_filter::InNode::ValuesHolder> ConvertArray(const iceberg::filter::ArrayHolder& array) {
  std::vector<ToType> result;
  result.reserve(array.Size());
  for (const auto& maybe_value : array.GetValue<value_type>()) {
    if (!maybe_value.has_value()) {
      return arrow::Status::ExecutionError("ConvertArray: null is not supported");
    }
    if constexpr (value_type == ValueType::kNumeric) {
      result.emplace_back(maybe_value.value().value);
    } else {
      result.emplace_back(maybe_value.value());
    }
  }
  return result;
}

arrow::Result<file_filter::InNode::ValuesHolder> ProcessConstArray(const iceberg::filter::ArrayHolder& array) {
  switch (array.GetValueType()) {
    case ValueType::kBool:
      return ConvertArray<bool, ValueType::kBool>(array);
    case ValueType::kInt2:
      return ConvertArray<int64_t, ValueType::kInt2>(array);
    case ValueType::kInt4:
      return ConvertArray<int64_t, ValueType::kInt4>(array);
    case ValueType::kInt8:
      return ConvertArray<int64_t, ValueType::kInt8>(array);
    case ValueType::kFloat4:
      return ConvertArray<double, ValueType::kFloat4>(array);
    case ValueType::kFloat8:
      return ConvertArray<double, ValueType::kFloat8>(array);
    case ValueType::kTime:
      return ConvertArray<int64_t, ValueType::kTime>(array);
    case ValueType::kTimestamp:
      return ConvertArray<int64_t, ValueType::kTimestamp>(array);
    case ValueType::kTimestamptz:
      return ConvertArray<int64_t, ValueType::kTimestamptz>(array);
    case ValueType::kDate:
      return ConvertArray<int64_t, ValueType::kDate>(array);
    case ValueType::kNumeric:
      return ConvertArray<std::string, ValueType::kNumeric>(array);
    case ValueType::kString:
      return ConvertArray<std::string, ValueType::kString>(array);
    default:
      return arrow::Status::ExecutionError("ProcessConstArray: unexpected type ",
                                           static_cast<int>(array.GetValueType()));
  }
}

arrow::Result<FNodePtr> ProcessScalarOverArrayFunction(
    std::shared_ptr<iceberg::filter::ScalarOverArrayFunctionNode> node, const Context& ctx) {
  auto func_id = node->function_signature.function_id;
  bool use_or = node->use_or;

  if (!((func_id == FunctionID::kEqual && use_or) || (func_id == FunctionID::kNotEqual && !use_or))) {
    return arrow::Status::ExecutionError("ScalarOverArrayFunctionNode: operation must be 'in' or 'not in'");
  }

  auto scalar = node->scalar;
  if (scalar->node_type != NodeType::kVariable) {
    return arrow::Status::ExecutionError("ScalarOverArrayFunctionNode: scalar must be variable");
  }

  auto scalar_node = std::static_pointer_cast<iceberg::filter::VariableNode>(scalar);
  std::string column_name = scalar_node->column_name;

  const auto& array = node->array;
  ARROW_ASSIGN_OR_RAISE(auto values, ProcessConstArray(array));
  if (std::holds_alternative<std::vector<int64_t>>(values)) {
    ARROW_RETURN_NOT_OK(TryToCastIntValueArray(std::get<std::vector<int64_t>>(values), array.GetValueType(),
                                               scalar_node->value_type, ctx));
  }

  return std::make_shared<file_filter::InNode>(FunctionID::kEqual == func_id ? Operator::kIn : Operator::kNotIn,
                                               column_name, std::move(values));
}

arrow::Result<FNodePtr> ProcessLogical(std::shared_ptr<iceberg::filter::LogicalNode> node, const Context& ctx) {
  switch (node->operation) {
    case iceberg::filter::LogicalNode::Operation::kNot: {
      ARROW_ASSIGN_OR_RAISE(auto res, ProcessRoot(node->arguments[0], ctx));
      return std::make_shared<file_filter::NotNode>(res);
    }
    case iceberg::filter::LogicalNode::Operation::kOr:
    case iceberg::filter::LogicalNode::Operation::kAnd: {
      std::vector<FNodePtr> nodes;
      nodes.reserve(node->arguments.size());
      for (auto arg : node->arguments) {
        ARROW_ASSIGN_OR_RAISE(auto res, ProcessRoot(arg, ctx));
        nodes.emplace_back(res);
      }
      Operator op = node->operation == iceberg::filter::LogicalNode::Operation::kAnd ? Operator::kAnd : Operator::kOr;
      FNodePtr result = nodes[0];
      for (size_t i = 1; i < nodes.size(); ++i) {
        result = std::make_shared<file_filter::LogicalBinaryNode>(op, result, nodes[i]);
      }
      return result;
    }
  }
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string("Internal error in tea."));
}

arrow::Result<FNodePtr> ProcessFunction(std::shared_ptr<iceberg::filter::FunctionNode> node, const Context& ctx) {
  if (node->function_signature.function_id == FunctionID::kIsNull) {
    if (node->arguments.size() != 1) {
      return arrow::Status::ExecutionError("ProcessFunction: expected 1 argument for isnull");
    }
    auto arg = node->arguments[0];
    if (arg->node_type != NodeType::kVariable) {
      return arrow::Status::ExecutionError("ProcessFunction: expected variable as argument for isnull");
    }
    return std::make_shared<file_filter::NullNode>(
        Operator::kIsNull, std::static_pointer_cast<iceberg::filter::VariableNode>(arg)->column_name);
  }

  // Only comparisons between var and const are supported
  if (node->arguments.size() != 2) {
    return arrow::Status::ExecutionError("ProcessFunction: expected 2 arguments");
  }

  auto lhs = node->arguments[0];
  auto rhs = node->arguments[1];

  ARROW_ASSIGN_OR_RAISE(auto op, FunctionIDToOperator(node->function_signature.function_id));

  if (lhs->node_type == NodeType::kConst && rhs->node_type == NodeType::kVariable) {
    std::swap(lhs, rhs);
    ARROW_ASSIGN_OR_RAISE(op, FlipOperator(op));
  }

  if (lhs->node_type != NodeType::kVariable) {
    return arrow::Status::ExecutionError("ProcessFunction: expected lhs to be variable");
  }
  if (rhs->node_type != NodeType::kConst) {
    return arrow::Status::ExecutionError("ProcessFunction: expected rhs to be const");
  }

  auto var_node = std::static_pointer_cast<iceberg::filter::VariableNode>(lhs);
  auto const_node = std::static_pointer_cast<iceberg::filter::ConstNode>(rhs);

  std::string column_name = var_node->column_name;
  ARROW_ASSIGN_OR_RAISE(auto value, ProcessConst(const_node));

  if (std::holds_alternative<int64_t>(value)) {
    ARROW_RETURN_NOT_OK(
        TryToCastIntValue(std::get<int64_t>(value), const_node->value.GetValueType(), var_node->value_type, ctx));
  }

  if (op == Operator::kStartsWith || op == Operator::kNotStartsWith) {
    ARROW_ASSIGN_OR_RAISE(std::string starts_with_string,
                          GetStartsWithStringFromLikeString(std::get<std::string>(value)));
    return std::make_shared<file_filter::StartsWithNode>(op, column_name, std::move(starts_with_string));
  }

  return std::make_shared<file_filter::ComparisonNode>(op, column_name, std::move(value));
}

arrow::Result<FNodePtr> ProcessRoot(NodePtr node, const Context& ctx) {
  switch (node->node_type) {
    case NodeType::kVariable:
      return ProcessVariable(std::static_pointer_cast<iceberg::filter::VariableNode>(node));
    case NodeType::kFunction:
      return ProcessFunction(std::static_pointer_cast<iceberg::filter::FunctionNode>(node), ctx);
    case NodeType::kLogical:
      return ProcessLogical(std::static_pointer_cast<iceberg::filter::LogicalNode>(node), ctx);
    case NodeType::kScalarOverArrayFunction:
      return ProcessScalarOverArrayFunction(
          std::static_pointer_cast<iceberg::filter::ScalarOverArrayFunctionNode>(node), ctx);
    default:
      return arrow::Status::ExecutionError("ProcessRoot: unexpected type ", static_cast<int>(node->node_type));
  }
}

}  // namespace

std::string GetTeapotFileFilter(NodePtr root, const Context& ctx) {
  if (!root) {
    return "";
  }

  std::vector<NodePtr> nodes_to_process;

  if (root->node_type == NodeType::kLogical &&
      std::static_pointer_cast<iceberg::filter::LogicalNode>(root)->operation ==
          iceberg::filter::LogicalNode::Operation::kAnd) {
    nodes_to_process = std::static_pointer_cast<iceberg::filter::LogicalNode>(root)->arguments;
  } else {
    nodes_to_process = {root};
  }

  std::vector<FNodePtr> nodes_to_apply;
  for (const auto& node : nodes_to_process) {
    auto process_result = ProcessRoot(node, ctx);
    if (process_result.ok()) {
      nodes_to_apply.emplace_back(process_result.ValueUnsafe());
    }
  }

  if (nodes_to_apply.empty()) {
    return "";
  }

  FNodePtr result = std::move(nodes_to_apply[0]);
  for (size_t i = 1; i < nodes_to_apply.size(); ++i) {
    result = std::make_shared<tea::file_filter::LogicalBinaryNode>(tea::file_filter::Operator::kAnd, result,
                                                                   nodes_to_apply[i]);
  }

  tea::file_filter::Serializer serializer;
  auto json_result = serializer.Serialize(result);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  json_result.Accept(writer);
  std::string res = buffer.GetString();
  return res;
}

}  // namespace tea::filter
