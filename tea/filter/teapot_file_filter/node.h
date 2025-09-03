#pragma once

#include <memory>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace tea::file_filter {

enum class Operator {
  kLt,
  kLtEq,
  kGt,
  kGtEq,
  kEq,
  kNotEq,
  kIsNull,
  kIsNotNull,
  kAnd,
  kOr,
  kNot,
  kStartsWith,
  kNotStartsWith,
  kIn,
  kNotIn,
  kUnknown
};

std::string OperatorToString(Operator op);
Operator StringToOperator(const std::string& str);

enum class NodeType { kComparison, kNull, kLogicalBinary, kNot, kStartsWith, kIn };

struct Node {
  NodeType node_type;

  explicit Node(NodeType type) : node_type(type) {}
};

using NodePtr = std::shared_ptr<Node>;

struct ComparisonNode : public Node {
  using ValueHolder = std::variant<bool, int64_t, double, std::string>;

  Operator op;
  std::string column_name;
  ValueHolder value;

  ComparisonNode(Operator op, std::string column_name, ValueHolder value)
      : Node(NodeType::kComparison), op(op), column_name(std::move(column_name)), value(std::move(value)) {}
};

struct NullNode : public Node {
  Operator op;
  std::string column_name;

  NullNode(Operator op, std::string column_name) : Node(NodeType::kNull), op(op), column_name(std::move(column_name)) {}
};

struct LogicalBinaryNode : public Node {
  Operator op;
  NodePtr left_arg;
  NodePtr right_arg;

  LogicalBinaryNode(Operator op, NodePtr lhs, NodePtr rhs)
      : Node(NodeType::kLogicalBinary), op(op), left_arg(lhs), right_arg(rhs) {}
};

struct NotNode : public Node {
  NodePtr arg;

  explicit NotNode(NodePtr arg) : Node(NodeType::kNot), arg(arg) {}
};

struct StartsWithNode : public Node {
  Operator op;
  std::string column_name;
  std::string value;

  StartsWithNode(Operator op, std::string column_name, std::string value)
      : Node(NodeType::kStartsWith), op(op), column_name(std::move(column_name)), value(std::move(value)) {}
};

struct InNode : public Node {
  using ValuesHolder =
      std::variant<std::vector<bool>, std::vector<int64_t>, std::vector<double>, std::vector<std::string>>;

  Operator op;
  std::string column_name;
  ValuesHolder values;

  InNode(Operator op, std::string column_name, ValuesHolder array)
      : Node(NodeType::kIn), op(op), column_name(std::move(column_name)), values(std::move(array)) {}

  template <typename T>
  inline const std::vector<T>& GetArray() const {
    return std::get<std::vector<T>>(values);
  }
};

}  // namespace tea::file_filter
