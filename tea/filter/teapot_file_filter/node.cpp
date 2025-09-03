#include "tea/filter/teapot_file_filter/node.h"

#include "arrow/status.h"

namespace tea {

namespace file_filter {

namespace {

struct OperatorInfo {
  Operator op;
  std::string_view str;
};

static constexpr OperatorInfo op_to_str[] = {{Operator::kIsNull, "is-null"},
                                             {Operator::kIsNotNull, "not-null"},
                                             {Operator::kLt, "lt"},
                                             {Operator::kLtEq, "lt-eq"},
                                             {Operator::kGt, "gt"},
                                             {Operator::kGtEq, "gt-eq"},
                                             {Operator::kEq, "eq"},
                                             {Operator::kNotEq, "not-eq"},
                                             {Operator::kIn, "in"},
                                             {Operator::kNotIn, "not-in"},
                                             {Operator::kNot, "not"},
                                             {Operator::kAnd, "and"},
                                             {Operator::kOr, "or"},
                                             {Operator::kStartsWith, "starts-with"},
                                             {Operator::kNotStartsWith, "not-starts-with"}};

}  // namespace

std::string OperatorToString(Operator op) {
  for (const auto& now : op_to_str) {
    if (now.op == op) {
      return std::string(now.str);
    }
  }
  throw arrow::Status::ExecutionError("OperatorToString: unsupported operator ", static_cast<int>(op));
}

Operator StringToOperator(std::string_view str) {
  for (const auto& now : op_to_str) {
    if (now.str == str) {
      return now.op;
    }
  }
  throw arrow::Status::ExecutionError("StringToOperator: unsupported string ", str);
}

}  // namespace file_filter

}  // namespace tea
