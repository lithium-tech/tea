#pragma once

#include <memory>

#include "rapidjson/document.h"

#include "tea/filter/teapot_file_filter/node.h"

namespace tea::file_filter {

class Serializer {
 public:
  using Allocator = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;
  rapidjson::Value Serialize(NodePtr node);

 private:
  rapidjson::Value SerializeComparison(std::shared_ptr<ComparisonNode> node);
  rapidjson::Value SerializeNull(std::shared_ptr<NullNode> node);
  rapidjson::Value SerializeLogicalBinary(std::shared_ptr<LogicalBinaryNode> node);
  rapidjson::Value SerializeNot(std::shared_ptr<NotNode> node);
  rapidjson::Value SerializeStartsWith(std::shared_ptr<StartsWithNode> node);
  rapidjson::Value SerializeIn(std::shared_ptr<InNode> node);

  Allocator allocator_;

  void AddType(rapidjson::Value& value, Operator op);
};

}  // namespace tea::file_filter
