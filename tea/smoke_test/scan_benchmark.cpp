#include <arrow/filesystem/s3fs.h>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <string>
#include <vector>
#include <numeric>
#include <algorithm>
#include <thread>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_split.h"
#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"

#include "tea/smoke_test/environment.h"
#include "tea/smoke_test/pq.h"
#include "tea/smoke_test/stats_state.h"
#include "tea/test_utils/location.h"
#include "tea/test_utils/common.h"
#include "tea/test_utils/metadata.h"

ABSL_FLAG(std::string, db, "gperov", "Hive Metastore database name");
ABSL_FLAG(std::string, table, "test", "Iceberg table name");
ABSL_FLAG(std::string, columns, "a int8, b int8", "Greenplum column definitions (e.g. 'a int8, b int8')");
ABSL_FLAG(std::string, profile, "iceberg_samovar", "Tea profile (e.g. 'iceberg_samovar', 'iceberg_samovar_slow')");
ABSL_FLAG(std::string, table_type, "foreign", "Greenplum table type: 'foreign' or 'external'");
ABSL_FLAG(std::string, query, "count(*)", "Query expression (e.g. 'count(*)', 'a, b')");
ABSL_FLAG(std::string, where, "", "Optional WHERE condition");
ABSL_FLAG(int32_t, warmup, 1, "Number of warmup iterations");
ABSL_FLAG(int32_t, iterations, 3, "Number of timed benchmark iterations");
ABSL_FLAG(bool, include_master, false, "Include master node (-1) in per-node stats breakdown");
ABSL_FLAG(bool, print_proto, true, "Print raw protobuf debug metrics from segments");
ABSL_FLAG(int32_t, generate_files, 0, "If > 0, generate an Iceberg table with N data files (tasks)");
ABSL_FLAG(int32_t, rows_per_file, 1000, "Number of rows per generated data file");

namespace tea {
namespace {

double DurationToMs(const ::google::protobuf::Duration& d) {
  return (d.seconds() * 1000.0) + (d.nanos() / 1e6);
}

std::vector<GreenplumColumnInfo> ParseColumns(const std::string& cols_str) {
  std::vector<GreenplumColumnInfo> result;
  std::vector<std::string> col_defs = absl::StrSplit(cols_str, ',', absl::SkipEmpty());
  for (const auto& col_def : col_defs) {
    std::string trimmed = std::string(absl::StripAsciiWhitespace(col_def));
    std::vector<std::string> parts = absl::StrSplit(trimmed, absl::ByAnyChar(" \t"), absl::SkipWhitespace());
    if (parts.size() >= 2) {
      result.push_back(GreenplumColumnInfo{.name = parts[0], .type = parts[1]});
    }
  }
  return result;
}

struct IterationResult {
  double wall_clock_ms = 0.0;
  std::vector<stats_state::StatsRequest> stats_requests;
  int64_t total_redis_requests = 0;
  int64_t total_redis_errors = 0;
  int64_t total_rows_read = 0;
  int64_t total_files_read = 0;
};

void PrintSeparator(char ch = '=', int len = 80) {
  std::cout << std::string(len, ch) << "\n";
}

}  // namespace
}  // namespace tea

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  const std::string db = absl::GetFlag(FLAGS_db);
  const std::string table = absl::GetFlag(FLAGS_table);
  const std::string columns_str = absl::GetFlag(FLAGS_columns);
  const std::string profile = absl::GetFlag(FLAGS_profile);
  const std::string table_type = absl::GetFlag(FLAGS_table_type);
  const std::string query_expr = absl::GetFlag(FLAGS_query);
  const std::string where_cond = absl::GetFlag(FLAGS_where);
  const int32_t warmup_count = std::max(0, absl::GetFlag(FLAGS_warmup));
  const int32_t iterations_count = std::max(1, absl::GetFlag(FLAGS_iterations));
  const bool include_master = absl::GetFlag(FLAGS_include_master);
  const bool print_proto = absl::GetFlag(FLAGS_print_proto);
  const int32_t generate_files = std::max(0, absl::GetFlag(FLAGS_generate_files));
  const int32_t rows_per_file = std::max(1, absl::GetFlag(FLAGS_rows_per_file));

  const std::string gp_table_name = "tea_bench_tbl";

  std::unique_ptr<tea::LocalFileWriter> file_writer;
  std::unique_ptr<tea::IcebergMetadataWriter> metadata_writer;
  std::optional<tea::Location> generated_location;

  if (generate_files > 0) {
    auto maybe_hms = tea::Environment::GetHiveMetastoreClient();
    if (!maybe_hms.ok()) {
      std::cerr << "[ERROR] Failed to connect to HMS: " << maybe_hms.status().ToString() << "\n";
      return 1;
    }
    auto hms_client = maybe_hms.ValueUnsafe();
    file_writer = std::make_unique<tea::LocalFileWriter>();
    const std::string gen_tbl = "bench_gen_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count() % 1000000);
    metadata_writer = std::make_unique<tea::IcebergMetadataWriter>(gen_tbl, hms_client, profile);

    std::cout << "[INFO] Dynamically generating " << generate_files << " data file(s) with "
              << rows_per_file << " rows each...\n";

    for (int32_t i = 0; i < generate_files; ++i) {
      tea::OptionalVector<int64_t> col_a;
      tea::OptionalVector<int64_t> col_b;
      col_a.reserve(rows_per_file);
      col_b.reserve(rows_per_file);
      for (int32_t j = 0; j < rows_per_file; ++j) {
        col_a.emplace_back(static_cast<int64_t>(i * rows_per_file + j));
        col_b.emplace_back(static_cast<int64_t>(j));
      }
      auto col_a_obj = tea::MakeInt64Column("a", 1, std::move(col_a));
      auto col_b_obj = tea::MakeInt64Column("b", 2, std::move(col_b));
      auto write_res = file_writer->WriteFile({col_a_obj, col_b_obj}, tea::IFileWriter::Hints{});
      if (!write_res.ok()) {
        std::cerr << "[ERROR] Failed to write data file " << i << ": " << write_res.status().ToString() << "\n";
        return 1;
      }
      auto add_st = metadata_writer->AddDataFiles({write_res.ValueUnsafe()});
      if (!add_st.ok()) {
        std::cerr << "[ERROR] Failed to add file " << i << " to metadata: " << add_st.ToString() << "\n";
        return 1;
      }
    }

    auto fin_res = metadata_writer->Finalize();
    if (!fin_res.ok()) {
      std::cerr << "[ERROR] Failed to finalize generated table: " << fin_res.status().ToString() << "\n";
      return 1;
    }
    generated_location = fin_res.MoveValueUnsafe();
    std::cout << "[INFO] Generated Iceberg table ready.\n";
  }

  tea::Location location = generated_location.has_value()
      ? *generated_location
      : tea::Location(tea::IcebergLocation(db, table, tea::Options{.profile = profile}));

  tea::PrintSeparator('=');
  std::cout << " TEA SCAN BENCHMARK \n";
  tea::PrintSeparator('=');

  if (generate_files > 0) {
    std::cout << "Target Table   : Generated (" << generate_files << " files, " << rows_per_file << " rows/file)\n";
  } else {
    std::cout << "Target Table   : " << db << "." << table << "\n";
  }

  std::cout << "Location URI   : " << location.ToString() << "\n";
  std::cout << "Profile        : " << profile << "\n";
  std::cout << "Table Type     : " << table_type << "\n";
  std::cout << "Columns        : " << columns_str << "\n";
  std::cout << "Query          : SELECT " << query_expr << " FROM " << gp_table_name;
  if (!where_cond.empty()) {
    std::cout << " WHERE " << where_cond;
  }
  std::cout << "\n";
  std::cout << "Iterations     : " << iterations_count << " (warmup: " << warmup_count << ")\n";
  tea::PrintSeparator('-');

  tea::StatsState stats_state;

  tea::pq::PGconnWrapper conn(PQconnectdb("dbname = postgres"));
  if (PQstatus(conn.Ptr()) != CONNECTION_OK) {
    std::cerr << "[ERROR] Failed to connect to Greenplum: " << PQerrorMessage(conn.Ptr()) << "\n";
    return 1;
  }

  tea::pq::Command("DROP FOREIGN TABLE IF EXISTS " + gp_table_name).Run(conn).ok();
  tea::pq::Command("DROP EXTERNAL TABLE IF EXISTS " + gp_table_name).Run(conn).ok();

  auto columns = tea::ParseColumns(columns_str);
  if (columns.empty()) {
    std::cerr << "[ERROR] No valid columns parsed from: " << columns_str << "\n";
    return 1;
  }

  std::optional<tea::pq::DropTableDefer> table_defer;
  if (table_type == "foreign") {
    auto create_query = tea::pq::CreateForeignTableQuery(columns, gp_table_name, location);
    auto res = create_query.Run(conn);
    if (!res.ok()) {
      std::cerr << "[ERROR] Failed to create foreign table: " << res.status().ToString() << "\n";
      return 1;
    }
    table_defer.emplace(res.MoveValueUnsafe());
  } else {
    auto create_query = tea::pq::CreateExternalTableQuery(columns, gp_table_name, location);
    auto res = create_query.Run(conn);
    if (!res.ok()) {
      std::cerr << "[ERROR] Failed to create external table: " << res.status().ToString() << "\n";
      return 1;
    }
    table_defer.emplace(res.MoveValueUnsafe());
  }
  std::cout << "[INFO] Successfully created " << table_type << " table '" << gp_table_name << "' in Greenplum.\n\n";

  auto run_query = [&]() -> arrow::Result<tea::pq::ScanResult> {
    if (where_cond.empty()) {
      return tea::pq::TableScanQuery(gp_table_name, query_expr).Run(conn);
    } else {
      return tea::pq::TableScanQuery(gp_table_name, query_expr).SetWhere(where_cond).Run(conn);
    }
  };

  if (warmup_count > 0) {
    std::cout << "[INFO] Running " << warmup_count << " warmup iteration(s)...\n";
    for (int32_t w = 0; w < warmup_count; ++w) {
      stats_state.ClearStats();
      auto res = run_query();
      if (!res.ok()) {
        std::cerr << "[WARN] Warmup " << (w + 1) << " failed: " << res.status().ToString() << "\n";
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      stats_state.GetStatsRequests(true);
    }
    std::cout << "[INFO] Warmup completed.\n\n";
  }

  std::vector<tea::IterationResult> results;
  results.reserve(iterations_count);

  std::cout << std::fixed << std::setprecision(2);

  for (int32_t i = 1; i <= iterations_count; ++i) {
    stats_state.ClearStats();

    auto start_time = std::chrono::steady_clock::now();
    auto maybe_scan = run_query();
    auto end_time = std::chrono::steady_clock::now();

    if (!maybe_scan.ok()) {
      std::cerr << "[ERROR] Iteration " << i << " failed: " << maybe_scan.status().ToString() << "\n";
      continue;
    }

    double duration_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    auto requests = stats_state.GetStatsRequests(include_master);

    tea::IterationResult iter_res;
    iter_res.wall_clock_ms = duration_ms;
    iter_res.stats_requests = std::move(requests);

    for (const auto& req : iter_res.stats_requests) {
      const auto& s = req.stats();
      iter_res.total_redis_requests += s.samovar().samovar_requests_count();
      iter_res.total_redis_errors += s.samovar().samovar_errors_count();
      iter_res.total_rows_read += s.data().rows_read();
      iter_res.total_files_read += s.data().data_files_read();
    }

    std::cout << ">>> Iteration " << i << " / " << iterations_count << ":\n";
    std::cout << "  * Wall-clock Query Duration : " << iter_res.wall_clock_ms << " ms\n";
    std::cout << "  * Segments Reported         : " << iter_res.stats_requests.size() << "\n";
    std::cout << "  * Redis (Samovar) Activity  :\n";
    std::cout << "      - Total Redis Requests  : " << iter_res.total_redis_requests << "\n";
    std::cout << "      - Total Redis Errors    : " << iter_res.total_redis_errors << "\n";

    for (const auto& req : iter_res.stats_requests) {
      int seg_id = req.scan_id().segment_id();
      const auto& sam = req.stats().samovar();
      double resp_ms = tea::DurationToMs(sam.samovar_total_response_duration_ticks());
      std::cout << "      - Node/Segment " << seg_id << " : "
                << sam.samovar_requests_count() << " requests, "
                << sam.samovar_errors_count() << " errors, "
                << "response duration: " << resp_ms << " ms, "
                << "fetched tasks: " << sam.samovar_fetched_tasks_count() << "\n";
    }

    std::cout << "  * Data / Iceberg Scan Stats :\n";
    std::cout << "      - Total Rows Read       : " << iter_res.total_rows_read << "\n";
    std::cout << "      - Total Files Read      : " << iter_res.total_files_read << "\n";
    std::cout << "\n";

    results.push_back(std::move(iter_res));
  }

  if (!results.empty()) {
    std::vector<double> timings;
    std::vector<int64_t> redis_reqs;
    for (const auto& r : results) {
      timings.push_back(r.wall_clock_ms);
      redis_reqs.push_back(r.total_redis_requests);
    }
    double min_ms = *std::min_element(timings.begin(), timings.end());
    double max_ms = *std::max_element(timings.begin(), timings.end());
    double avg_ms = std::accumulate(timings.begin(), timings.end(), 0.0) / timings.size();
    double avg_redis = std::accumulate(redis_reqs.begin(), redis_reqs.end(), 0.0) / redis_reqs.size();

    tea::PrintSeparator('=');
    std::cout << " BENCHMARK SUMMARY\n";
    tea::PrintSeparator('=');
    std::cout << "Query Execution Time (Wall-Clock):\n";
    std::cout << "  * Min Duration      : " << min_ms << " ms\n";
    std::cout << "  * Max Duration      : " << max_ms << " ms\n";
    std::cout << "  * Avg Duration      : " << avg_ms << " ms\n";
    std::cout << "Redis (Samovar) Requests:\n";
    std::cout << "  * Avg Total Requests: " << avg_redis << "\n";
    std::cout << "  * Segment Breakdown : Reported above per iteration\n";
    tea::PrintSeparator('=');
  }

  if (print_proto && !results.empty()) {
    std::cout << "\n";
    tea::PrintSeparator('-');
    std::cout << " PROTOBUF METRICS DUMP (From Last Iteration)\n";
    tea::PrintSeparator('-');
    const auto& last_reqs = results.back().stats_requests;
    if (last_reqs.empty()) {
      std::cout << "  (No segment stats received - check if debug.test_stats is enabled in tea-config.json)\n";
    }
    for (const auto& req : last_reqs) {
      std::cout << "[Segment " << req.scan_id().segment_id() << " Proto Metrics]\n";
      std::cout << req.stats().DebugString();
      tea::PrintSeparator('-');
    }
  }

  if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
    arrow::fs::FinalizeS3().ok();
  }

  return 0;
}
