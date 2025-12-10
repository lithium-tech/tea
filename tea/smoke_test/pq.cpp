#include "tea/smoke_test/pq.h"

#include <algorithm>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <utility>
#include <vector>

#include "arrow/status.h"

namespace tea {

namespace pq {

namespace {

inline PGresult* PQexec(PGconn* conn, const std::string& query) { return PQexec(conn, query.c_str()); }

ScanResult PGResultToScanResult(const PGresultWrapper& select_result) {
  std::vector<std::string> headers;
  std::vector<std::vector<std::string>> values;
  int n_fields = PQnfields(select_result.Ptr());
  int n_tuples = PQntuples(select_result.Ptr());

  headers.reserve(n_fields);
  for (int i = 0; i < n_fields; i++) {
    headers.emplace_back(PQfname(select_result.Ptr(), i));
  }

  values.reserve(n_tuples);
  for (int i = 0; i < n_tuples; i++) {
    std::vector<std::string> row;
    for (int j = 0; j < n_fields; j++) {
      row.emplace_back(PQgetvalue(select_result.Ptr(), i, j));
    }
    values.emplace_back(std::move(row));
  }

  return ScanResult(std::move(headers), std::move(values));
}

std::vector<uint64_t> GetSortedIndices(const ScanResult& scan_result) {
  std::vector<uint64_t> result(scan_result.values.size());
  std::iota(result.begin(), result.end(), 0);
  std::sort(result.begin(), result.end(),
            [&](uint64_t lhs, uint64_t rhs) { return scan_result.values[lhs] < scan_result.values[rhs]; });
  return result;
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& arr) {
  os << "[";
  for (size_t i = 0; i < arr.size(); ++i) {
    if (i != 0) {
      os << ", ";
    }
    os << arr[i];
  }
  os << "]";
  return os;
}

}  // namespace

std::ostream& operator<<(std::ostream& os, const ScanResult& result) {
  return os << "headers: " << result.headers << ", values = " << result.values;
}

bool ScanResult::operator==(const ScanResult& other) const {
  if (headers.size() != other.headers.size() || values.size() != other.values.size()) {
    return false;
  }
  std::vector<uint64_t> indices_lhs = GetSortedIndices(*this);
  std::vector<uint64_t> indices_rhs = GetSortedIndices(other);
  for (uint64_t i = 0; i < indices_lhs.size(); ++i) {
    if (values[indices_lhs[i]] != other.values[indices_rhs[i]]) {
      return false;
    }
  }
  return true;
}

arrow::Status Command::Run(PGconnWrapper& conn) {
  PGresultWrapper select_result(PQexec(conn.Ptr(), query_.c_str()));
  if (PQresultStatus(select_result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("Command failed: ", PQerrorMessage(conn.Ptr()));
  }
  return arrow::Status::OK();
}

arrow::Result<std::pair<LazyScanResult, QueryOrCommand::Type>> QueryOrCommand::Run(PGconnWrapper& conn) {
  PGresultWrapper select_result(PQexec(conn.Ptr(), query_.c_str()));
  if (PQresultStatus(select_result.Ptr()) == PGRES_COMMAND_OK ||
      PQresultStatus(select_result.Ptr()) == PGRES_EMPTY_QUERY) {
    return std::pair<LazyScanResult, QueryOrCommand::Type>(LazyScanResult{std::nullopt},
                                                           QueryOrCommand::Type::kCommand);
  }
  if (PQresultStatus(select_result.Ptr()) == PGRES_TUPLES_OK) {
    return std::pair<LazyScanResult, QueryOrCommand::Type>(LazyScanResult{std::move(select_result)},
                                                           QueryOrCommand::Type::kQuery);
  }
  return arrow::Status::ExecutionError("QueryOrCommand failed: ", PQerrorMessage(conn.Ptr()));
}

ScanResult LazyScanResult::ToScanResult() const {
  if (!result) {
    return ScanResult({}, {});
  }
  return PGResultToScanResult(*result);
}

arrow::Result<ScanResult> Query::Run(PGconnWrapper& conn) {
  PGresultWrapper select_result(PQexec(conn.Ptr(), query_.c_str()));
  if (PQresultStatus(select_result.Ptr()) == PGRES_COMMAND_OK) {
    return ScanResult({}, {});
  }
  if (PQresultStatus(select_result.Ptr()) != PGRES_TUPLES_OK) {
    return arrow::Status::ExecutionError("Query failed: ", PQerrorMessage(conn.Ptr()));
  }
  return PGResultToScanResult(select_result);
}

bool AsyncQuery::Run(PGconnWrapper& conn) {
  if (!PQsendQuery(conn.Ptr(), query_.c_str())) {
    PQfinish(conn.Ptr());
    return false;
  }

  return true;
}

void AsyncQuery::CancelQuery(PGconnWrapper& conn) {
  char errbuf[256];
  PGcancel* cancel = PQgetCancel(conn.Ptr());

  if (!cancel) {
    throw std::runtime_error("Can not cancel query");
  }

  if (!PQcancel(cancel, errbuf, sizeof(errbuf))) {
    throw std::runtime_error("Can not cancel query");
  }
  PQfreeCancel(cancel);
}

bool AsyncTableScanQuery::Run(PGconnWrapper& conn) {
  std::stringstream ss;
  ss << "SELECT ";
  for (size_t i = 0; i < retrieved_exprs_.size(); ++i) {
    if (i != 0) {
      ss << ", ";
    }
    ss << retrieved_exprs_[i];
  }
  ss << " FROM " << table_name_ << ";";

  auto result_query = std::string(ss.str());
  if (!PQsendQuery(conn.Ptr(), result_query.c_str())) {
    PQfinish(conn.Ptr());
    return false;
  }

  return true;
}

void AsyncTableScanQuery::CancelQuery(PGconnWrapper& conn) {
  char errbuf[256];
  PGcancel* cancel = PQgetCancel(conn.Ptr());

  if (!cancel) {
    throw std::runtime_error("Can not cancel query");
  }

  if (!PQcancel(cancel, errbuf, sizeof(errbuf))) {
    throw std::runtime_error("Can not cancel query");
  }
  PQfreeCancel(cancel);
}

arrow::Result<ScanResult> TableScanQuery::Run(PGconnWrapper& conn) {
  std::stringstream ss;
  ss << "SELECT ";
  for (size_t i = 0; i < retrieved_exprs_.size(); ++i) {
    if (i != 0) {
      ss << ", ";
    }
    ss << retrieved_exprs_[i];
  }
  ss << " FROM " << table_name_;
  if (!condition_.empty()) {
    ss << " WHERE " << condition_;
  }
  ss << ";";

  PGresultWrapper select_result(PQexec(conn.Ptr(), ss.str().c_str()));
  if (PQresultStatus(select_result.Ptr()) != PGRES_TUPLES_OK) {
    return arrow::Status::ExecutionError("SELECT failed: ", PQerrorMessage(conn.Ptr()));
  }
  return PGResultToScanResult(select_result);
}

arrow::Status DropForeignTableQuery::Run(PGconnWrapper& conn) {
  if (table_name_ == "") {
    return arrow::Status::OK();
  }
  std::stringstream query;
  query << "DROP FOREIGN TABLE IF EXISTS " << table_name_;
  query << " CASCADE ;";

  PGresultWrapper result(PQexec(conn.Ptr(), query.str().c_str()));
  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("DROP TABLE failed: ", PQerrorMessage(conn.Ptr()));
  }
  return arrow::Status::OK();
}

arrow::Status DropExternalTableQuery::Run(PGconnWrapper& conn) {
  if (table_name_ == "") {
    return arrow::Status::OK();
  }
  std::stringstream query;
  query << "DROP EXTERNAL TABLE IF EXISTS " << table_name_;
  query << " CASCADE ;";

  PGresultWrapper result(PQexec(conn.Ptr(), query.str().c_str()));
  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("DROP TABLE failed: ", PQerrorMessage(conn.Ptr()));
  }
  return arrow::Status::OK();
}

arrow::Status DropNativeTableQuery::Run(PGconnWrapper& conn) {
  if (table_name_ == "") {
    return arrow::Status::OK();
  }
  std::stringstream query;
  query << "DROP TABLE IF EXISTS " << table_name_;
  query << " CASCADE ;";

  PGresultWrapper result(PQexec(conn.Ptr(), query.str().c_str()));
  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("DROP TABLE failed: ", PQerrorMessage(conn.Ptr()));
  }
  return arrow::Status::OK();
}

arrow::Result<DropExternalTableQueryDefer> CreateExternalTableQuery::Run(PGconnWrapper& conn) {
  std::stringstream query;
  query << "CREATE READABLE EXTERNAL TABLE " << table_name_ << "(\n";
  for (size_t i = 0; i < column_info_.size(); ++i) {
    const auto& column_info = column_info_[i];
    query << column_info.name << " ";
    query << column_info.type;
    if (i + 1 != column_info_.size()) {
      query << ",\n";
    } else {
      query << ")\n";
    }
  }
  query << "LOCATION('" << location_.ToString() << "')\n";
  query << "FORMAT 'custom' (formatter = tea_import)\n";
  query << "ENCODING 'UTF8';\n";

  PGresultWrapper result(PQexec(conn.Ptr(), query.str().c_str()));
  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("CREATE EXTERNAL TABLE failed: ", PQerrorMessage(conn.Ptr()));
  }

  return DropExternalTableQueryDefer(DropExternalTableQuery(table_name_), conn);
}

arrow::Result<DropForeignTableQueryDefer> CreateForeignTableQuery::Run(PGconnWrapper& conn) {
  std::stringstream query;
  query << "CREATE FOREIGN TABLE " << table_name_ << "(\n";
  for (size_t i = 0; i < column_info_.size(); ++i) {
    const auto& column_info = column_info_[i];
    query << column_info.name << " ";
    query << column_info.type;
    if (i + 1 != column_info_.size()) {
      query << ",\n";
    } else {
      query << ")\n";
    }
  }
  query << "SERVER tea_server\n";
  query << "OPTIONS(\n";
  query << "location '" << location_.ToString() << "'\n";
  query << ")";
  query << ";";

  PGresultWrapper result(PQexec(conn.Ptr(), query.str().c_str()));
  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("CREATE EXTERNAL TABLE failed: ", PQerrorMessage(conn.Ptr()));
  }

  return DropForeignTableQueryDefer(DropForeignTableQuery(table_name_), conn);
}

arrow::Result<DropNativeTableQueryDefer> CreateNativeTableQuery::Run(PGconnWrapper& conn) {
  std::stringstream query;
  query << "CREATE TABLE " << table_name_ << "(\n";
  for (size_t i = 0; i < column_info_.size(); ++i) {
    const auto& column_info = column_info_[i];
    query << column_info.name << " ";
    query << column_info.type;
    if (i + 1 != column_info_.size()) {
      query << ",\n";
    } else {
      query << ")\n";
    }
  }
  query << ";";

  PGresultWrapper result(PQexec(conn.Ptr(), query.str().c_str()));
  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("CREATE TABLE failed: ", PQerrorMessage(conn.Ptr()));
  }

  return DropNativeTableQueryDefer(DropNativeTableQuery(table_name_), conn);
}

arrow::Status SetTimeZone(PGconnWrapper& conn, int hour) {
  PGresultWrapper res(PQexec(conn.Ptr(), "SET TIME ZONE " + std::to_string(hour) + ";"));
  if (PQresultStatus(res.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("SET TIME ZONE failed: ", PQerrorMessage(conn.Ptr()));
  }
  return arrow::Status::OK();
}

arrow::Status DropTea(PGconnWrapper& conn) {
  PGresultWrapper result(PQexec(conn.Ptr(), "DROP EXTENSION IF EXISTS tea CASCADE;"));

  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("DROP EXTENSION failed: ", PQerrorMessage(conn.Ptr()));
  }

  return arrow::Status::OK();
}

arrow::Status CreateTea(PGconnWrapper& conn) {
  PGresultWrapper result(PQexec(conn.Ptr(), "CREATE EXTENSION tea;"));

  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("CREATE EXTENSION failed: ", PQerrorMessage(conn.Ptr()));
  }

  return arrow::Status::OK();
}

arrow::Status DropColumn(PGconnWrapper& conn, const std::string& table_name, const std::string& col_name) {
  PGresultWrapper result(PQexec(conn.Ptr(), "ALTER TABLE " + table_name + " DROP COLUMN " + col_name + ";"));

  if (PQresultStatus(result.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("DROP COLUMN failed: ", PQerrorMessage(conn.Ptr()));
  }

  return arrow::Status::OK();
}

arrow::Result<int> GetTableOid(PGconnWrapper& conn, const std::string& table_name) {
  PGresultWrapper result(PQexec(conn.Ptr(), "SELECT '" + table_name + "'::regclass::oid;"));

  if (PQresultStatus(result.Ptr()) != PGRES_TUPLES_OK) {
    return arrow::Status::ExecutionError("GetTableOid failed: ", PQerrorMessage(conn.Ptr()));
  }

  ScanResult scan_result = PGResultToScanResult(result);

  return std::stoi(scan_result.values[0][0]);
}

arrow::Status Analyze(PGconnWrapper& conn, const std::string& table_name) {
  PGresultWrapper res(PQexec(conn.Ptr(), "ANALYZE " + table_name + ";"));
  if (PQresultStatus(res.Ptr()) != PGRES_COMMAND_OK) {
    return arrow::Status::ExecutionError("ANALYZE failed: ", PQerrorMessage(conn.Ptr()));
  }
  return arrow::Status::OK();
}

int GetSegmentsCount(PGconnWrapper& conn) {
  auto maybe_segments_num_query = pq::Query("SELECT count(*) FROM gp_toolkit.gp_disk_free").Run(conn);
  if (!maybe_segments_num_query.ok()) {
    throw maybe_segments_num_query.status();
  }
  const auto segments_num = std::stoi(maybe_segments_num_query.MoveValueUnsafe().values[0][0]);
  return segments_num;
}

}  // namespace pq

}  // namespace tea
