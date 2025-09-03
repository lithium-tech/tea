#include "tea/filter/teapot_file_filter/serializer.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"

namespace tea::file_filter {

namespace {
using Allocator = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;
}  // namespace

void Serializer::AddType(rapidjson::Value& result, Operator op) {
  result.AddMember("type", rapidjson::Value(OperatorToString(op).c_str(), allocator_), allocator_);
}

rapidjson::Value Serializer::SerializeComparison(std::shared_ptr<ComparisonNode> node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddType(result, node->op);
  result.AddMember("term", rapidjson::Value(node->column_name.c_str(), allocator_), allocator_);
  std::visit(
      [&](auto&& arg) {
        using ValueType = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<std::string, ValueType>) {
          result.AddMember("value", rapidjson::Value(arg.c_str(), allocator_), allocator_);
        } else {
          result.AddMember("value", arg, allocator_);
        }
      },
      node->value);
  return result;
}

rapidjson::Value Serializer::SerializeNull(std::shared_ptr<NullNode> node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddType(result, node->op);
  result.AddMember("term", rapidjson::Value(node->column_name.c_str(), allocator_), allocator_);
  return result;
}

rapidjson::Value Serializer::SerializeLogicalBinary(std::shared_ptr<LogicalBinaryNode> node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddType(result, node->op);
  result.AddMember("left", Serialize(node->left_arg), allocator_);
  result.AddMember("right", Serialize(node->right_arg), allocator_);
  return result;
}

rapidjson::Value Serializer::SerializeNot(std::shared_ptr<NotNode> node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddType(result, Operator::kNot);
  result.AddMember("child", Serialize(node->arg), allocator_);
  return result;
}

rapidjson::Value Serializer::SerializeStartsWith(std::shared_ptr<StartsWithNode> node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddType(result, node->op);
  result.AddMember("term", rapidjson::Value(node->column_name.c_str(), allocator_), allocator_);
  result.AddMember("value", rapidjson::Value(node->value.c_str(), allocator_), allocator_);
  return result;
}

rapidjson::Value Serializer::SerializeIn(std::shared_ptr<InNode> node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddType(result, node->op);
  result.AddMember("term", rapidjson::Value(node->column_name.c_str(), allocator_), allocator_);
  rapidjson::Value values(rapidjson::kArrayType);
  std::visit(
      [&](auto&& arg) {
        using ValueType = std::decay_t<decltype(arg)>;
        for (size_t i = 0; i < arg.size(); ++i) {
          if constexpr (std::is_same_v<std::vector<std::string>, ValueType>) {
            values.PushBack(rapidjson::Value(arg[i].c_str(), allocator_), allocator_);
          } else {
            values.PushBack(arg[i], allocator_);
          }
        }
      },
      node->values);
  result.AddMember("values", std::move(values), allocator_);
  return result;
}

rapidjson::Value Serializer::Serialize(NodePtr node) {
  switch (node->node_type) {
    case NodeType::kComparison:
      return SerializeComparison(std::static_pointer_cast<ComparisonNode>(node));
    case NodeType::kNull:
      return SerializeNull(std::static_pointer_cast<NullNode>(node));
    case NodeType::kLogicalBinary:
      return SerializeLogicalBinary(std::static_pointer_cast<LogicalBinaryNode>(node));
    case NodeType::kNot:
      return SerializeNot(std::static_pointer_cast<NotNode>(node));
    case NodeType::kStartsWith:
      return SerializeStartsWith(std::static_pointer_cast<StartsWithNode>(node));
    case NodeType::kIn:
      return SerializeIn(std::static_pointer_cast<InNode>(node));
    default:
      throw arrow::Status::ExecutionError("Serialize: internal error in tea. Unexpected node type ",
                                          static_cast<int32_t>(node->node_type));
  }
}

}  // namespace tea::file_filter
