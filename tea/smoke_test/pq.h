#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "libpq-fe.h"  // NOLINT build/include_subdir

#include "tea/test_utils/column.h"
#include "tea/test_utils/location.h"

namespace tea {

namespace pq {

class PGresultWrapper {
 public:
  explicit PGresultWrapper(PGresult* result) : result_(result) {}

  PGresultWrapper(const PGresultWrapper& other) = delete;

  PGresultWrapper(PGresultWrapper&& other) : result_(other.result_) { other.result_ = nullptr; }

  PGresultWrapper& operator=(PGresult* result) {
    Clear();
    result_ = result;
    return *this;
  }

  PGresultWrapper& operator=(const PGresultWrapper& other) = delete;

  PGresultWrapper& operator=(PGresultWrapper&& other) {
    Clear();
    result_ = other.result_;
    other.result_ = nullptr;
    return *this;
  }

  ~PGresultWrapper() { Clear(); }

  void Clear() {
    if (result_) {
      PQclear(result_);
      result_ = nullptr;
    }
  }

  const PGresult* Ptr() const { return result_; }

 private:
  PGresult* result_ = nullptr;
};

class PGconnWrapper {
 public:
  PGconnWrapper() : conn_(nullptr) {}

  explicit PGconnWrapper(PGconn* conn) : conn_(conn) {}

  PGconnWrapper(const PGconnWrapper& other) = delete;

  PGconnWrapper(PGconnWrapper&& other) : conn_(other.conn_) { other.conn_ = nullptr; }

  PGconnWrapper& operator=(const PGconnWrapper& other) = delete;

  PGconnWrapper& operator=(PGconnWrapper&& other) {
    Finish();
    conn_ = other.conn_;
    other.conn_ = nullptr;
    return *this;
  }

  PGconn* Ptr() { return conn_; }

  ~PGconnWrapper() { Finish(); }

  void Finish() {
    if (conn_) {
      PQfinish(conn_);
      conn_ = nullptr;
    }
  }

 private:
  PGconn* conn_;
};

struct ScanResult {
  std::vector<std::string> headers;
  std::vector<std::vector<std::string>> values;

  ScanResult(const std::vector<std::string>& headers, const std::vector<std::vector<std::string>>& values)
      : headers(headers), values(values) {}

  friend std::ostream& operator<<(std::ostream& os, const ScanResult& result);

  bool operator==(const ScanResult& other) const;
};

struct LazyScanResult {
  std::optional<PGresultWrapper> result;

  ScanResult ToScanResult() const;
};

class Command {
 public:
  explicit Command(const std::string& query) : query_(query) {}

  arrow::Status Run(PGconnWrapper& conn);

 private:
  std::string query_;
};

class QueryOrCommand {
 public:
  explicit QueryOrCommand(const std::string& query) : query_(query) {}

  enum class Type { kCommand, kQuery };

  arrow::Result<std::pair<LazyScanResult, Type>> Run(PGconnWrapper& conn);

 private:
  std::string query_;
};

class Query {
 public:
  explicit Query(const std::string& query) : query_(query) {}

  arrow::Result<ScanResult> Run(PGconnWrapper& conn);

 private:
  std::string query_;
};

class AsyncQuery {
 public:
  explicit AsyncQuery(const std::string& query) : query_(query) {}

  bool Run(PGconnWrapper& conn);

  void CancelQuery(PGconnWrapper& conn);

 private:
  std::string query_;
};

class AsyncTableScanQuery {
 public:
  explicit AsyncTableScanQuery(const std::string& table_name) : table_name_(table_name), retrieved_exprs_({"*"}) {}

  bool Run(PGconnWrapper& conn);

  void CancelQuery(PGconnWrapper& conn);

 private:
  std::string table_name_;
  std::vector<std::string> retrieved_exprs_;
};

class TableScanQuery {
 public:
  explicit TableScanQuery(const std::string& table_name) : table_name_(table_name), retrieved_exprs_({"*"}) {}

  TableScanQuery(const std::string& table_name, const std::string& retrieved_expr)
      : table_name_(table_name), retrieved_exprs_({retrieved_expr}) {}

  TableScanQuery(const std::string& table_name, const std::vector<std::string>& retrieved_exprs)
      : table_name_(table_name), retrieved_exprs_(retrieved_exprs) {}

  TableScanQuery SetWhere(const std::string& condition) && {
    condition_ = condition;
    return std::move(*this);
  }

  arrow::Result<ScanResult> Run(PGconnWrapper& conn);

 private:
  std::string table_name_;
  std::string condition_;
  std::vector<std::string> retrieved_exprs_;
};

class DropForeignTableQuery {
 public:
  explicit DropForeignTableQuery(const std::string& table_name) : table_name_(table_name) {}
  DropForeignTableQuery(const DropForeignTableQuery&) = delete;
  DropForeignTableQuery& operator=(const DropForeignTableQuery&) = delete;
  DropForeignTableQuery(DropForeignTableQuery&&) = default;

  arrow::Status Run(PGconnWrapper& conn);

 private:
  std::string table_name_;
};

class DropExternalTableQuery {
 public:
  explicit DropExternalTableQuery(const std::string& table_name) : table_name_(table_name) {}
  DropExternalTableQuery(const DropExternalTableQuery&) = delete;
  DropExternalTableQuery& operator=(const DropExternalTableQuery&) = delete;
  DropExternalTableQuery(DropExternalTableQuery&&) = default;

  arrow::Status Run(PGconnWrapper& conn);

 private:
  std::string table_name_;
};

class DropNativeTableQuery {
 public:
  explicit DropNativeTableQuery(const std::string& table_name) : table_name_(table_name) {}
  DropNativeTableQuery(const DropNativeTableQuery&) = delete;
  DropNativeTableQuery& operator=(const DropNativeTableQuery&) = delete;
  DropNativeTableQuery(DropNativeTableQuery&&) = default;

  arrow::Status Run(PGconnWrapper& conn);

 private:
  std::string table_name_;
};

template <typename DropTableQuery>
class DropTableQueryDefer {
 public:
  DropTableQueryDefer(const DropTableQueryDefer&) = delete;
  DropTableQueryDefer& operator=(const DropTableQueryDefer&) = delete;

  DropTableQueryDefer(DropTableQueryDefer&&) = default;

  DropTableQueryDefer(DropTableQuery&& query, PGconnWrapper& conn) : query_(std::move(query)), conn_(conn) {}

  ~DropTableQueryDefer() {
    if (query_.has_value()) {
      query_->Run(conn_).ok();
    }
  }

 private:
  std::optional<DropTableQuery> query_;
  PGconnWrapper& conn_;
};

using DropExternalTableQueryDefer = DropTableQueryDefer<DropExternalTableQuery>;
using DropForeignTableQueryDefer = DropTableQueryDefer<DropForeignTableQuery>;
using DropNativeTableQueryDefer = DropTableQueryDefer<DropNativeTableQuery>;

class DropTableDefer {
 public:
  explicit DropTableDefer(DropExternalTableQueryDefer&& val) : defer_(std::move(val)) {}
  explicit DropTableDefer(DropNativeTableQueryDefer&& val) : defer_(std::move(val)) {}
  explicit DropTableDefer(DropForeignTableQueryDefer&& val) : defer_(std::move(val)) {}

 private:
  std::variant<DropNativeTableQueryDefer, DropExternalTableQueryDefer, DropForeignTableQueryDefer> defer_;
};

class CreateExternalTableQuery {
 public:
  CreateExternalTableQuery(const std::vector<GreenplumColumnInfo>& column_info, const std::string& table_name,
                           const Location& location)
      : column_info_(column_info), table_name_(table_name), location_(location) {}

  arrow::Result<DropExternalTableQueryDefer> Run(PGconnWrapper& conn);

 private:
  std::vector<GreenplumColumnInfo> column_info_;
  std::string table_name_;
  Location location_;
};

class CreateForeignTableQuery {
 public:
  CreateForeignTableQuery(const std::vector<GreenplumColumnInfo>& column_info, const std::string& table_name,
                          const Location& location)
      : column_info_(column_info), table_name_(table_name), location_(location) {}

  arrow::Result<DropForeignTableQueryDefer> Run(PGconnWrapper& conn);

 private:
  std::vector<GreenplumColumnInfo> column_info_;
  std::string table_name_;
  Location location_;
};

class CreateNativeTableQuery {
 public:
  CreateNativeTableQuery(const std::vector<GreenplumColumnInfo>& column_info, const std::string& table_name)
      : column_info_(column_info), table_name_(table_name) {}

  arrow::Result<DropNativeTableQueryDefer> Run(PGconnWrapper& conn);

 private:
  std::vector<GreenplumColumnInfo> column_info_;
  std::string table_name_;
};

arrow::Status DropTea(PGconnWrapper& conn);
arrow::Status CreateTea(PGconnWrapper& conn);

arrow::Result<int> GetTableOid(PGconnWrapper& conn, const std::string& table_name);

arrow::Status DropColumn(PGconnWrapper& conn, const std::string& table_name, const std::string& col_name);

arrow::Status SetTimeZone(PGconnWrapper& conn, int hour);

arrow::Status Analyze(PGconnWrapper& conn, const std::string& table_name);

int GetSegmentsCount(PGconnWrapper& conn);

}  // namespace pq

}  // namespace tea
