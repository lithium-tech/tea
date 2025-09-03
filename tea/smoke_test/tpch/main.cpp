#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <numeric>
#include <random>
#include <stdexcept>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/internal/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_split.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "gen/src/generators.h"
#include "gen/tpch/dataset.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"
#include "parquet/arrow/reader.h"

#include "tea/smoke_test/fragment_info.h"
#include "tea/smoke_test/mock_teapot.h"
#include "tea/smoke_test/perf_test/table.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/stats_state.h"
#include "tea/smoke_test/test_base.h"
#include "tea/test_utils/column.h"
#include "tea/test_utils/metadata.h"

namespace params {
enum class TableType { kNative, kExternal, kForeign };
enum class StatsMode { kNone, kReltuples, kJoinDistinct, kAllDistinct, kWithAnalyze };
enum class Optimizer { kOrca, kLegacy };
enum class OrcaCostModel { kLegacy, kCalibrated, kExperimental };

struct GeneratorParams {
  int scale_factor = 1;
  int files_per_table = 1;
  int seed = 0;
  int threads = 1;
};

std::string ToString(TableType table_type) {
  switch (table_type) {
    case TableType::kNative:
      return "native";
    case TableType::kExternal:
      return "external";
    case TableType::kForeign:
      return "foreign";
  }
  throw std::runtime_error(__PRETTY_FUNCTION__);
}

std::string ToString(StatsMode stats_mode) {
  switch (stats_mode) {
    case StatsMode::kNone:
      return "none";
    case StatsMode::kReltuples:
      return "reltuples";
    case StatsMode::kJoinDistinct:
      return "join_distinct";
    case StatsMode::kAllDistinct:
      return "all_distinct";
    case StatsMode::kWithAnalyze:
      return "with_analyze";
  }
  throw std::runtime_error(__PRETTY_FUNCTION__);
}

std::string ToString(Optimizer optimizer) {
  switch (optimizer) {
    case Optimizer::kOrca:
      return "orca";
    case Optimizer::kLegacy:
      return "legacy";
  }
  throw std::runtime_error(__PRETTY_FUNCTION__);
}

std::string ToString(OrcaCostModel cost_model) {
  switch (cost_model) {
    case OrcaCostModel::kLegacy:
      return "legacy";
    case OrcaCostModel::kCalibrated:
      return "calibrated";
    case OrcaCostModel::kExperimental:
      return "experimental";
  }
  throw std::runtime_error(__PRETTY_FUNCTION__);
}

}  // namespace params

namespace tea {
namespace {

const std::vector<GreenplumColumnInfo> kSupplierGPSchema{
    GreenplumColumnInfo{.name = "s_suppkey", .type = "int4"},
    GreenplumColumnInfo{.name = "s_name", .type = "text"},
    GreenplumColumnInfo{.name = "s_address", .type = "text"},
    GreenplumColumnInfo{.name = "s_nationkey", .type = "int4"},
    GreenplumColumnInfo{.name = "s_phone", .type = "text"},
    GreenplumColumnInfo{.name = "s_acctbal", .type = "decimal(12, 2)"},
    GreenplumColumnInfo{.name = "s_comment", .type = "text"}};

const std::vector<GreenplumColumnInfo> kLineitemGPSchema{
    GreenplumColumnInfo{.name = "l_orderkey", .type = "int8"},
    GreenplumColumnInfo{.name = "l_partkey", .type = "int4"},
    GreenplumColumnInfo{.name = "l_suppkey", .type = "int4"},
    GreenplumColumnInfo{.name = "l_linenumber", .type = "int4"},
    GreenplumColumnInfo{.name = "l_quantity", .type = "decimal(12, 2)"},
    GreenplumColumnInfo{.name = "l_extendedprice", .type = "decimal(12, 2)"},
    GreenplumColumnInfo{.name = "l_discount", .type = "decimal(12, 2)"},
    GreenplumColumnInfo{.name = "l_tax", .type = "decimal(12, 2)"},
    GreenplumColumnInfo{.name = "l_returnflag", .type = "text"},
    GreenplumColumnInfo{.name = "l_linestatus", .type = "text"},
    GreenplumColumnInfo{.name = "l_shipdate", .type = "date"},
    GreenplumColumnInfo{.name = "l_commitdate", .type = "date"},
    GreenplumColumnInfo{.name = "l_receiptdate", .type = "date"},
    GreenplumColumnInfo{.name = "l_shipinstruct", .type = "text"},
    GreenplumColumnInfo{.name = "l_shipmode", .type = "text"},
    GreenplumColumnInfo{.name = "l_comment", .type = "text"}};

const std::vector<GreenplumColumnInfo> kOrdersGPSchema{
    GreenplumColumnInfo{.name = "o_orderkey", .type = "int8"},
    GreenplumColumnInfo{.name = "o_custkey", .type = "int4"},
    GreenplumColumnInfo{.name = "o_orderstatus", .type = "text"},
    GreenplumColumnInfo{.name = "o_totalprice", .type = "decimal(12, 2)"},
    GreenplumColumnInfo{.name = "o_orderdate", .type = "date"},
    GreenplumColumnInfo{.name = "o_orderpriority", .type = "text"},
    GreenplumColumnInfo{.name = "o_clerk", .type = "text"},
    GreenplumColumnInfo{.name = "o_shippriority", .type = "int4"},
    GreenplumColumnInfo{.name = "o_comment", .type = "text"}};

const std::vector<GreenplumColumnInfo> kNationGPSChema{GreenplumColumnInfo{.name = "n_nationkey", .type = "int4"},
                                                       GreenplumColumnInfo{.name = "n_name", .type = "text"},
                                                       GreenplumColumnInfo{.name = "n_regionkey", .type = "int4"},
                                                       GreenplumColumnInfo{.name = "n_comment", .type = "text"}};

const std::vector<GreenplumColumnInfo> kRegionGPSChema{GreenplumColumnInfo{.name = "r_regionkey", .type = "int4"},
                                                       GreenplumColumnInfo{.name = "r_name", .type = "text"},
                                                       GreenplumColumnInfo{.name = "r_comment", .type = "text"}};

const std::vector<GreenplumColumnInfo> kPartGPSChema{
    GreenplumColumnInfo{.name = "p_partkey", .type = "int4"},
    GreenplumColumnInfo{.name = "p_name", .type = "text"},
    GreenplumColumnInfo{.name = "p_mfgr", .type = "text"},
    GreenplumColumnInfo{.name = "p_brand", .type = "text"},
    GreenplumColumnInfo{.name = "p_type", .type = "text"},
    GreenplumColumnInfo{.name = "p_size", .type = "int4"},
    GreenplumColumnInfo{.name = "p_container", .type = "text"},
    GreenplumColumnInfo{.name = "p_retailprice", .type = "decimal(12,2)"},
    GreenplumColumnInfo{.name = "p_comment", .type = "text"}};

const std::vector<GreenplumColumnInfo> kPartsuppGPSChema{
    GreenplumColumnInfo{.name = "ps_partkey", .type = "int4"},
    GreenplumColumnInfo{.name = "ps_suppkey", .type = "int4"},
    GreenplumColumnInfo{.name = "ps_availqty", .type = "int4"},
    GreenplumColumnInfo{.name = "ps_supplycost", .type = "decimal(12,2)"},
    GreenplumColumnInfo{.name = "ps_comment", .type = "text"}};

const std::vector<GreenplumColumnInfo> kCustomerGPSChema{
    GreenplumColumnInfo{.name = "c_custkey", .type = "int4"},
    GreenplumColumnInfo{.name = "c_name", .type = "text"},
    GreenplumColumnInfo{.name = "c_address", .type = "text"},
    GreenplumColumnInfo{.name = "c_nationkey", .type = "int4"},
    GreenplumColumnInfo{.name = "c_phone", .type = "text"},
    GreenplumColumnInfo{.name = "c_acctbal", .type = "decimal(12,2)"},
    GreenplumColumnInfo{.name = "c_mktsegment", .type = "text"},
    GreenplumColumnInfo{.name = "c_comment", .type = "text"}};

namespace tables {
constexpr std::string_view kLineitem = "lineitem";
constexpr std::string_view kOrders = "orders";
constexpr std::string_view kPart = "part";
constexpr std::string_view kPartsupp = "partsupp";
constexpr std::string_view kRegion = "region";
constexpr std::string_view kSupplier = "supplier";
constexpr std::string_view kNation = "nation";
constexpr std::string_view kCustomer = "customer";
}  // namespace tables

arrow::Result<std::vector<GreenplumColumnInfo>> TableNameToSchema(const std::string& name) {
  if (name == tables::kLineitem) {
    return kLineitemGPSchema;
  } else if (name == tables::kOrders) {
    return kOrdersGPSchema;
  } else if (name == tables::kNation) {
    return kNationGPSChema;
  } else if (name == tables::kRegion) {
    return kRegionGPSChema;
  } else if (name == tables::kPart) {
    return kPartGPSChema;
  } else if (name == tables::kPartsupp) {
    return kPartsuppGPSChema;
  } else if (name == tables::kCustomer) {
    return kCustomerGPSChema;
  } else if (name == tables::kSupplier) {
    return kSupplierGPSchema;
  }
  return arrow::Status::ExecutionError("Unknown table name: ", name);
}

arrow::Result<std::vector<std::string>> GetColumnsForJoins(const std::string& name) {
  if (name == tables::kLineitem) {
    return std::vector<std::string>{"l_orderkey", "l_partkey", "l_suppkey"};
  } else if (name == tables::kOrders) {
    return std::vector<std::string>{"o_orderkey", "o_custkey"};
  } else if (name == tables::kNation) {
    return std::vector<std::string>{"n_nationkey", "n_regionkey"};
  } else if (name == tables::kRegion) {
    return std::vector<std::string>{"r_regionkey"};
  } else if (name == tables::kPart) {
    return std::vector<std::string>{"p_partkey"};
  } else if (name == tables::kPartsupp) {
    return std::vector<std::string>{"ps_partkey", "ps_suppkey"};
  } else if (name == tables::kCustomer) {
    return std::vector<std::string>{"c_custkey", "c_nationkey"};
  } else if (name == tables::kSupplier) {
    return std::vector<std::string>{"s_suppkey", "s_nationkey"};
  }
  return arrow::Status::ExecutionError("Unknown table name: ", name);
}

const std::vector<std::string_view> kTableNames = {tables::kCustomer, tables::kLineitem, tables::kNation,
                                                   tables::kOrders,   tables::kPart,     tables::kPartsupp,
                                                   tables::kRegion,   tables::kSupplier};

class TPCHTest {
 public:
  TPCHTest() : dir(), conn(PQconnectdb("dbname = postgres")) {
    auto status = pq::CreateTea(conn);
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
    }
  }

  arrow::Status AllowToModifySystemCatalog() {
#if PG_VERSION_MAJOR >= 9
    std::string query_allow_to_modify_catalog = "SET allow_system_table_mods = ON";
#else
    std::string query_allow_to_modify_catalog = "SET allow_system_table_mods = 'dml'";
#endif

    return pq::Command(query_allow_to_modify_catalog).Run(conn);
  }

  arrow::Status DropTablesIfExist() {
    std::cerr << "Deleting tables if exist" << std::endl;
    for (const auto& table_name : kTableNames) {
      // ignore errors
      pq::DropExternalTableQuery(std::string(table_name)).Run(conn).ok();
      pq::DropForeignTableQuery(std::string(table_name)).Run(conn).ok();
      pq::DropNativeTableQuery(std::string(table_name)).Run(conn).ok();
    }
    return arrow::Status::OK();
  }

  // get filenames matching pattern "{dir_path}/{table_name}[0-9]*.{extension}"
  std::vector<std::string> GetFilenamesFromDirectory(const std::string& table_name, const std::string& extension) {
    std::vector<std::string> result;
    for (const auto& entry : std::filesystem::directory_iterator(dir_path_.value())) {
      auto filename = entry.path().filename();
      auto stem = filename.stem().generic_string();
      bool is_match = stem.starts_with(table_name) && filename.extension().generic_string() == extension;
      if (is_match) {
        auto pos = table_name.size();
        while (pos < stem.size() && std::isdigit(stem[pos])) {
          ++pos;
        }
        is_match = pos == stem.size();
      }
      if (is_match) {
        result.emplace_back(entry.path().generic_string());
      }
    }

    return result;
  }

  arrow::Status CreateTable(const std::string& table_name, params::TableType table_type,
                            std::vector<pq::DropTableDefer>& defers) {
    ARROW_ASSIGN_OR_RAISE(auto schema, TableNameToSchema(table_name));
    switch (table_type) {
      case params::TableType::kExternal:
      case params::TableType::kForeign: {
        std::vector<FragmentInfo> fragments;
        std::vector<std::string> parquet_files = GetFilenamesFromDirectory(table_name, ".parquet");
        for (const auto& parquet_path : parquet_files) {
          std::string path_to_add = "file://" + parquet_path;
          auto offsets = GetParquetRowGroupOffsets(path_to_add);

          FragmentInfo fragment(path_to_add);
          for (size_t i = 0; i < offsets.size(); ++i) {
            auto position = offsets[i];
            auto length = (i + 1 == offsets.size() ? 0 : offsets[i + 1] - offsets[i]);
            FragmentInfo fragment_to_add = fragment.GetCopy().SetLength(length).SetPosition(position);
            fragments.emplace_back(fragment_to_add);
            std::cerr << table_name << ": " << path_to_add << " (" << position << ", " << length << ")" << std::endl;
          }
        }

        teapot.SetResponse("db." + table_name, TeapotExpectedResponse(std::move(fragments)));

        Location location(
            TeapotLocation("db", table_name, teapot.GetHost(), teapot.GetPort(), Options{.profile = profile_}));

        if (table_type == params::TableType::kExternal) {
          ARROW_ASSIGN_OR_RAISE(auto defer, pq::CreateExternalTableQuery(schema, table_name, location).Run(conn));
          defers.emplace_back(std::move(defer));
        } else {
          ARROW_ASSIGN_OR_RAISE(auto defer, pq::CreateForeignTableQuery(schema, table_name, location).Run(conn));
          defers.emplace_back(std::move(defer));
        }

        break;
      }

      case params::TableType::kNative: {
        ARROW_ASSIGN_OR_RAISE(auto defer, pq::CreateNativeTableQuery(schema, table_name).Run(conn));
        defers.emplace_back(std::move(defer));
        std::vector<std::string> csv_files = GetFilenamesFromDirectory(table_name, ".csv");
        for (const auto& csv_file : csv_files) {
          ARROW_RETURN_NOT_OK(pq::Command("COPY " + table_name + " FROM '" + csv_file + "' DELIMITER '|'").Run(conn));
        }
        break;
      }
    }
    return arrow::Status::OK();
  }

  arrow::Result<std::vector<pq::DropTableDefer>> CreateTables(params::TableType table_type) {
    std::vector<pq::DropTableDefer> table_defers;
    for (const auto& table_name : kTableNames) {
      ARROW_RETURN_NOT_OK(CreateTable(std::string(table_name), table_type, table_defers));
    }
    return table_defers;
  }

  static arrow::Result<std::string> ReadFile(const std::string& path) {
    std::ifstream stream(path);
    auto result = std::string((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
    if (result.empty()) {
      return arrow::Status::ExecutionError("ReadFile error: " + path);
    }
    return result;
  }

  static std::vector<std::string> SplitQuery(const std::string& s) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream token_stream(s);
    while (std::getline(token_stream, token, ';')) {
      tokens.push_back(token);
      token += "\n;";
    }
    return tokens;
  }

  static std::string MillisToString(std::chrono::milliseconds ms) {
    auto count = ms.count();
    auto seconds = count / 1000;
    auto milliseconds = count % 1000;
    std::string seconds_str = std::to_string(seconds);
    std::string milliseconds_str = std::to_string(milliseconds);
    while (milliseconds_str.length() < 3) {
      milliseconds_str = "0" + milliseconds_str;
    }
    return seconds_str + "." + milliseconds_str + "s";
  }

  std::string ExplainResultToString(pq::LazyScanResult& res) {
    auto scan_result = res.ToScanResult();
    std::string result;
    for (const auto& row : scan_result.values) {
      result += row[0] + "\n";
    }
    return result;
  }

  arrow::Result<pq::LazyScanResult> RunTpchQuery(std::string queries_dir, int query_num, bool explain,
                                                 bool explain_analyze) {
    std::optional<pq::LazyScanResult> result;

    ARROW_ASSIGN_OR_RAISE(std::string query,
                          ReadFile(std::string(queries_dir) + "/" + std::to_string(query_num) + ".sql"));
    // file may contain query and commands (create or drop some views, for example)
    for (auto& query_part : SplitQuery(query)) {
      if (explain) {
        auto maybe_explain_result = pq::QueryOrCommand("EXPLAIN " + std::string(query_part)).Run(conn);
        if (maybe_explain_result.ok()) {
          std::cerr << ExplainResultToString(maybe_explain_result.ValueUnsafe().first);
        }
      }
      if (explain_analyze) {
        auto maybe_explain_result = pq::QueryOrCommand("EXPLAIN ANALYZE " + std::string(query_part)).Run(conn);
        if (maybe_explain_result.ok()) {
          auto res = std::move(maybe_explain_result.ValueUnsafe().first);
          std::cerr << ExplainResultToString(res);
          result.emplace(std::move(res));
          continue;
        }
      }
      ARROW_ASSIGN_OR_RAISE(auto result_query, pq::QueryOrCommand(std::string(query_part)).Run(conn));
      if (result_query.second == pq::QueryOrCommand::Type::kQuery) {
        result.emplace(std::move(result_query.first));
      }
    }

    if (!result.has_value()) {
      return arrow::Status::ExecutionError("Invalid query ", query_num);
    }
    return std::move(result.value());
  }

  arrow::Status GenerateData(bool write_parquet, bool write_csv, params::GeneratorParams generator_params) {
    gen::WriteFlags write_flags{.output_dir = dir.path(), .write_parquet = write_parquet, .write_csv = write_csv};
    gen::GenerateFlags generate_flags{.scale_factor = generator_params.scale_factor,
                                      .arrow_batch_size = 8192,
                                      .seed = generator_params.seed,
                                      .files_per_table = generator_params.files_per_table,
                                      .use_equality_deletes = false,
                                      .equality_deletes_columns_count = 0,
                                      .equality_deletes_rows_scale = 0,
                                      .use_positional_deletes = false,
                                      .positional_deletes_rows_scale = 0,
                                      .threads_to_use = generator_params.threads};

    std::cerr << "Generating data" << std::endl;
    SetDataDirectory(dir.path());
    return gen::GenerateTPCH(write_flags, generate_flags);
  }

  arrow::Result<std::vector<pq::ScanResult>> RunTpchQueries(std::vector<int> queries_to_run, std::string queries_dir,
                                                            bool check_results, bool explain, bool explain_analyze) {
    std::vector<pq::ScanResult> result;
    std::chrono::milliseconds total_time{0};
    for (int query_num : queries_to_run) {
      auto start = std::chrono::high_resolution_clock::now();
      std::cerr << "Running query " << query_num << std::endl;
      auto maybe_result = RunTpchQuery(queries_dir, query_num, explain, explain_analyze);
      std::chrono::milliseconds millis{};
      if (!maybe_result.ok()) {
        std::cerr << maybe_result.status().message() << std::endl;
        result.emplace_back(pq::ScanResult({}, {}));
        auto end = std::chrono::high_resolution_clock::now();
        millis = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      } else {
        auto end = std::chrono::high_resolution_clock::now();
        millis = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        auto lazy_result = maybe_result.MoveValueUnsafe();
        if (check_results) {
          result.emplace_back(lazy_result.ToScanResult());
        } else {
          result.emplace_back(pq::ScanResult({}, {}));
        }
      }

      if (print_metrics_) {
        auto metrics = stats_state_.GetStats(false);
        for (auto& metric : metrics) {
          std::cerr << metric.DebugString() << std::endl;
        }
      }

      total_time += millis;
      std::cerr << "Running query " << query_num << " is done (" << MillisToString(millis)
                << "), rows in result = " << result.back().values.size() << std::endl;
    }
    std::cerr << "Total duration: " << MillisToString(total_time) << std::endl;
    return result;
  }

  arrow::Status SetStatsForTable(params::StatsMode stats_mode, std::string table_name) {
    if (stats_mode == params::StatsMode::kNone) {
      return arrow::Status::OK();
    }

    {
      std::string rows_str;
      ARROW_ASSIGN_OR_RAISE(auto result, pq::Query("SELECT count(*) FROM " + std::string(table_name)).Run(conn));
      rows_str = result.values[0][0];
      ARROW_RETURN_NOT_OK(pq::Command("UPDATE pg_class SET reltuples = " + rows_str + " WHERE relname = '" +
                                      std::string(table_name) + "'")
                              .Run(conn));
    }
    if (stats_mode == params::StatsMode::kReltuples) {
      return arrow::Status::OK();
    }

    if (stats_mode == params::StatsMode::kJoinDistinct || stats_mode == params::StatsMode::kAllDistinct) {
      ARROW_ASSIGN_OR_RAISE(auto schema, TableNameToSchema(std::string(table_name)));
      ARROW_ASSIGN_OR_RAISE(auto columns_for_join, GetColumnsForJoins(std::string(table_name)));

      for (size_t i = 0; i < schema.size(); ++i) {
        const auto& elem = schema[i];
        if (stats_mode == params::StatsMode::kJoinDistinct &&
            std::count(columns_for_join.begin(), columns_for_join.end(), elem.name) == 0) {
          continue;
        }

        int attno = i + 1;
        std::string attno_str = std::to_string(attno);

        std::string query_str = "SELECT '" + std::string(table_name) + "'::regclass::oid";
        std::cerr << query_str << std::endl;
        ARROW_ASSIGN_OR_RAISE(auto result_oid, pq::Query(query_str).Run(conn));
        std::string table_oid_str = result_oid.values[0][0];

        query_str = "SELECT count(distinct " + elem.name + ") FROM " + std::string(table_name);
        std::cerr << query_str << std::endl;
        ARROW_ASSIGN_OR_RAISE(auto result_distinct, pq::Query(query_str).Run(conn));
        std::string distinct_str = result_distinct.values[0][0];

        query_str = "DELETE FROM pg_statistic WHERE starelid = " + table_oid_str + " AND staattnum = " + attno_str;
        ARROW_RETURN_NOT_OK(pq::QueryOrCommand(query_str).Run(conn));

#if PG_VERSION_MAJOR >= 9
        query_str = "INSERT INTO pg_statistic VALUES (" + table_oid_str + ", " + attno_str + ", FALSE, 0, 4, " +
                    distinct_str + ", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)";
#else
        query_str = "INSERT INTO pg_statistic VALUES (" + table_oid_str + ", " + attno_str + ", 0, 4, " + distinct_str +
                    ", 0, 0, 0, 0, 0, 0, 0, 0)";

#endif
        std::cerr << query_str << std::endl;
        ARROW_RETURN_NOT_OK(pq::Command(query_str).Run(conn));
      }

      return arrow::Status::OK();
    }

    if (stats_mode == params::StatsMode::kWithAnalyze) {
      std::string query_str = "ANALYZE " + std::string(table_name);
      ARROW_RETURN_NOT_OK(pq::Command(query_str).Run(conn));
    }

    return arrow::Status::OK();
  }

  arrow::Status SetStats(params::StatsMode stats_mode) {
    for (const auto& table_name : kTableNames) {
      ARROW_RETURN_NOT_OK(SetStatsForTable(stats_mode, std::string(table_name)));
    }
    return arrow::Status::OK();
  }

  arrow::Status SetOptimizer(params::Optimizer optimizer) {
    std::string query;
    if (optimizer == params::Optimizer::kOrca) {
      query = "SET optimizer = on";
    } else {
      query = "SET optimizer = off";
    }
    return pq::Command(query).Run(conn);
  }

  arrow::Status SetOrcaCostModel(params::OrcaCostModel cost_model) {
    std::string query = "SET optimizer_cost_model = '" + params::ToString(cost_model) + "'";
    return pq::Command(query).Run(conn);
  }

  void SetDataDirectory(std::string str) { dir_path_.emplace(std::move(str)); }

  void SetProfile(std::string profile) { profile_ = std::move(profile); }

  void SetPrintMetrics(bool print_metrics) { print_metrics_ = print_metrics; }

  void SetVerbose() {
    auto status = tea::pq::Command("SET client_min_messages = 'LOG'").Run(conn);
    if (!status.ok()) {
      std::cerr << status << std::endl;
    }
  }

 private:
  std::string profile_;
  std::optional<std::string> dir_path_;
  ScopedTempDir dir;
  StatsState stats_state_;
  bool print_metrics_ = false;
  pq::PGconnWrapper conn;
  MockTeapot teapot;
};

arrow::Status Main(std::vector<params::TableType> table_types, std::vector<params::StatsMode> stats_modes,
                   std::vector<params::Optimizer> optimizers, std::vector<params::OrcaCostModel> orca_cost_models,
                   params::GeneratorParams generator_params, std::vector<int> queries_to_run, std::string queries_dir,
                   bool check_results, std::string data_dir, bool explain, bool explain_analyze, std::string profile,
                   bool print_metrics, bool verbose) {
  TPCHTest test;

#ifdef HAS_ARROW_CSV
  bool write_csv = true;
#else
  bool write_csv = false;
#endif

  if (std::find(table_types.begin(), table_types.end(), params::TableType::kNative) == table_types.end()) {
    write_csv = false;
  }

  test.SetProfile(profile);
  test.SetPrintMetrics(print_metrics);
  if (verbose) {
    test.SetVerbose();
  }

  ARROW_RETURN_NOT_OK(test.DropTablesIfExist());
  ARROW_RETURN_NOT_OK(test.AllowToModifySystemCatalog());

  if (data_dir.empty()) {
    ARROW_RETURN_NOT_OK(test.GenerateData(true, write_csv, generator_params));
  } else {
    test.SetDataDirectory(data_dir);
  }

  std::map<params::TableType, std::vector<pq::ScanResult>> table_type_to_result;

  for (const auto& table_type : table_types) {
    ARROW_ASSIGN_OR_RAISE(auto defers, test.CreateTables(table_type));
    for (const auto& stats_mode : stats_modes) {
      if (table_type == params::TableType::kExternal && stats_mode == params::StatsMode::kWithAnalyze) {
        continue;
      }
      ARROW_RETURN_NOT_OK(test.SetStats(stats_mode));
      auto optimizers_to_check = table_type != params::TableType::kForeign
                                     ? optimizers
                                     : std::vector<params::Optimizer>{params::Optimizer::kLegacy};
      for (const auto& optimizer : optimizers_to_check) {
        ARROW_RETURN_NOT_OK(test.SetOptimizer(optimizer));
        if (optimizer == params::Optimizer::kOrca) {
          for (const auto& orca_cost_model : orca_cost_models) {
            std::cerr << "Running configuration (" << params::ToString(table_type) << ", "
                      << params::ToString(stats_mode) << ", " << params::ToString(optimizer) << ", "
                      << params::ToString(orca_cost_model) << ")" << std::endl;

            ARROW_RETURN_NOT_OK(test.SetOrcaCostModel(orca_cost_model));

            ARROW_ASSIGN_OR_RAISE(
                auto res, test.RunTpchQueries(queries_to_run, queries_dir, check_results, explain, explain_analyze));
            table_type_to_result[table_type] = std::move(res);
          }
        } else {
          std::cerr << "Running configuration (" << params::ToString(table_type) << ", " << params::ToString(stats_mode)
                    << ", " << params::ToString(optimizer) << ")" << std::endl;

          ARROW_ASSIGN_OR_RAISE(
              auto res, test.RunTpchQueries(queries_to_run, queries_dir, check_results, explain, explain_analyze));
          table_type_to_result[table_type] = std::move(res);
        }
      }
    }
  }

  if (check_results) {
    std::cerr << "Checking results" << std::endl;

    for (size_t i = 1; i < table_type_to_result.size(); ++i) {
      std::cerr << "Comparing with configuration number " << i << std::endl;
      const auto& lhs = table_type_to_result.at(table_types[0]);
      const auto& rhs = table_type_to_result.at(table_types[i]);

      bool correct = true;
      if (lhs != rhs) {
        correct = false;
        std::cerr << "Results are different for table types " << static_cast<int>(table_types[0]) << " and "
                  << static_cast<int>(table_types[i]) << std::endl;
        for (size_t j = 0; j < lhs.size(); ++j) {
          if (j < rhs.size() && lhs[j] != rhs[j]) {
            std::cerr << "Query " << j + 1 << std::endl;
          }
        }
      }

      if (correct) {
        std::cerr << "Results for configuration " << i << " match results for configuration 0" << std::endl;
      }
    }
  }

  return arrow::Status::OK();
}

}  // namespace
}  // namespace tea

// AbslParseFlag and AbslUnparseFlag must be in the same namespace as std::vector<int>
namespace std {
bool AbslParseFlag(absl::string_view text, std::vector<int>* vec, std::string* error) {
  vec->clear();
  for (auto& oid_str : absl::StrSplit(text, ",")) {
    int id;
    if (!absl::SimpleAtoi(oid_str, &id)) {
      *error = "Unexpected id: " + std::string(oid_str);
      return false;
    }
    vec->emplace_back(id);
  }
  return true;
}

std::string AbslUnparseFlag(std::vector<int> vec) {
  std::stringstream ss;
  for (size_t i = 0; i < vec.size(); ++i) {
    if (i != 0) {
      ss << ",";
    }
    ss << vec[i];
  }
  return ss.str();
}
}  // namespace std

ABSL_FLAG(bool, native, false, "Run on native tables");
ABSL_FLAG(bool, external, false, "Run on external table");
ABSL_FLAG(bool, foreign, false, "Run on foreign table");

ABSL_FLAG(bool, no_stats, false, "");
ABSL_FLAG(bool, set_correct_reltuples, true, "");
ABSL_FLAG(bool, set_distinct_for_join_columns, false, "");
ABSL_FLAG(bool, set_distinct_for_every_column, false, "");
ABSL_FLAG(bool, run_with_analyze, false, "");

ABSL_FLAG(bool, run_with_orca, true, "");
ABSL_FLAG(bool, run_with_legacy_optimizer, false, "");

ABSL_FLAG(bool, run_with_orca_legacy_cost_model, false, "");
ABSL_FLAG(bool, run_with_orca_calibrated_cost_model, true, "");
ABSL_FLAG(bool, run_with_orca_experimental_cost_model, false, "");

ABSL_FLAG(bool, check_results, false, "");
ABSL_FLAG(bool, explain, false, "");
ABSL_FLAG(bool, explain_analyze, false, "");

ABSL_FLAG(std::string, queries_dir, "", "");
ABSL_FLAG(std::string, data_dir, "", "Directory with generated data");

ABSL_FLAG(int, seed, 0, "");
ABSL_FLAG(int, scale_factor, 1, "");
ABSL_FLAG(int, files_per_table, 1, "");
ABSL_FLAG(int, gen_threads, 1, "");

ABSL_FLAG(bool, print_metrics, false, "");

const std::vector<int> kDefaultQueriesToRun{1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11,
                                            12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22};
ABSL_FLAG(std::vector<int>, queries_to_run, {kDefaultQueriesToRun}, "");

ABSL_FLAG(std::string, profile, "", "");

ABSL_FLAG(bool, verbose, false, "Print logs to stderr");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::vector<params::TableType> table_types;
  std::vector<params::StatsMode> stats_modes;
  std::vector<params::Optimizer> optimizers;
  std::vector<params::OrcaCostModel> orca_cost_models;

  if (absl::GetFlag(FLAGS_native)) {
    table_types.emplace_back(params::TableType::kNative);
  }
  if (absl::GetFlag(FLAGS_external)) {
    table_types.emplace_back(params::TableType::kExternal);
  }
  if (absl::GetFlag(FLAGS_foreign)) {
    table_types.emplace_back(params::TableType::kForeign);

#if PG_VERSION_MAJOR < 9
    std::cerr << "Foreign tables are not supported" << std::endl;
    return 1;
#endif
  }

  if (absl::GetFlag(FLAGS_no_stats)) {
    stats_modes.emplace_back(params::StatsMode::kNone);
  }
  if (absl::GetFlag(FLAGS_set_correct_reltuples)) {
    stats_modes.emplace_back(params::StatsMode::kReltuples);
  }
  if (absl::GetFlag(FLAGS_set_distinct_for_join_columns)) {
    stats_modes.emplace_back(params::StatsMode::kJoinDistinct);
  }
  if (absl::GetFlag(FLAGS_set_distinct_for_every_column)) {
    stats_modes.emplace_back(params::StatsMode::kAllDistinct);
  }
  if (absl::GetFlag(FLAGS_run_with_analyze)) {
    stats_modes.emplace_back(params::StatsMode::kWithAnalyze);
  }

  if (absl::GetFlag(FLAGS_run_with_orca)) {
    optimizers.emplace_back(params::Optimizer::kOrca);
  }
  if (absl::GetFlag(FLAGS_run_with_legacy_optimizer)) {
    optimizers.emplace_back(params::Optimizer::kLegacy);
  }

  if (absl::GetFlag(FLAGS_run_with_orca_legacy_cost_model)) {
    orca_cost_models.emplace_back(params::OrcaCostModel::kLegacy);
  }
  if (absl::GetFlag(FLAGS_run_with_orca_calibrated_cost_model)) {
    orca_cost_models.emplace_back(params::OrcaCostModel::kCalibrated);
  }
  if (absl::GetFlag(FLAGS_run_with_orca_experimental_cost_model)) {
    orca_cost_models.emplace_back(params::OrcaCostModel::kExperimental);
  }

  std::string queries_dir = absl::GetFlag(FLAGS_queries_dir);
  if (queries_dir.empty()) {
    std::cerr << "queries_dir is not specified" << std::endl;
    return 1;
  }
  std::vector<int> queries_to_run = absl::GetFlag(FLAGS_queries_to_run);

  bool check_results = absl::GetFlag(FLAGS_check_results);
  bool explain = absl::GetFlag(FLAGS_explain);
  bool explain_analyze = absl::GetFlag(FLAGS_explain_analyze);

  const bool print_metrics = absl::GetFlag(FLAGS_print_metrics);

  std::string data_dir = absl::GetFlag(FLAGS_data_dir);

  params::GeneratorParams gen_params;
  gen_params.files_per_table = absl::GetFlag(FLAGS_files_per_table);
  gen_params.seed = absl::GetFlag(FLAGS_seed);
  gen_params.scale_factor = absl::GetFlag(FLAGS_scale_factor);
  gen_params.threads = absl::GetFlag(FLAGS_gen_threads);

  std::string profile = absl::GetFlag(FLAGS_profile);

  bool verbose = absl::GetFlag(FLAGS_verbose);

  auto status =
      tea::Main(table_types, stats_modes, optimizers, orca_cost_models, gen_params, queries_to_run, queries_dir,
                check_results, data_dir, explain, explain_analyze, profile, print_metrics, verbose);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return 1;
  }

  return 0;
}
