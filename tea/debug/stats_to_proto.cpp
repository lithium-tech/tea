#include "tea/debug/stats_to_proto.h"

#include <string>

#include "grpcpp/grpcpp.h"

#include "tea/debug/stats_state.grpc.pb.h"
#include "tea/debug/stats_state.pb.h"
#include "tea/util/signal_blocker.h"

namespace tea::debug {
namespace {

void SetDuration(DurationTicks duration_ticks, double ticks_per_second, ::google::protobuf::Duration* result) {
  constexpr int64_t kNanosPerSecond = 1e9;

  double seconds = duration_ticks / ticks_per_second;
  int64_t nanos = static_cast<int64_t>(seconds * kNanosPerSecond);
  result->set_seconds(nanos / kNanosPerSecond);
  result->set_nanos(nanos % kNanosPerSecond);
}

stats_state::StatsRequest StatsToProto(DurationTicks duration_ticks, double ticks_per_second,
                                       const PlannerStats& planner_stats, const ReaderStats& reader_stats,
                                       const S3Stats& s3_stats, const ExtStats& ext_stats,
                                       const iceberg::PositionalDeleteStats& positional_delete_stats,
                                       const iceberg::EqualityDeleteStats& equality_delete_stats, int segment_id) {
  stats_state::StatsRequest result;

  stats_state::ScanId* scan_id = result.mutable_scan_id();
  scan_id->set_segment_id(segment_id);

  stats_state::ExecutionStats* execution_stats = result.mutable_stats();

  stats_state::IcebergStats* iceberg_stats = execution_stats->mutable_iceberg();
  iceberg_stats->set_bytes_read(planner_stats.iceberg_bytes_read);
  iceberg_stats->set_files_read(planner_stats.iceberg_files_read);
  iceberg_stats->set_requests(planner_stats.iceberg_requests);

  stats_state::DataStats* data_stats = execution_stats->mutable_data();
  data_stats->set_data_files_read(reader_stats.data_files_read);
  data_stats->set_row_groups_read(reader_stats.row_groups_read);
  data_stats->set_row_groups_skipped_filter(reader_stats.row_groups_skipped_filter);

  data_stats->set_rows_read(reader_stats.rows_read);
  data_stats->set_rows_skipped_positional_delete(reader_stats.rows_skipped_positional_delete);
  data_stats->set_rows_skipped_equality_delete(reader_stats.rows_skipped_equality_delete);
  data_stats->set_rows_skipped_filter(reader_stats.rows_skipped_filter);
  data_stats->set_rows_skipped_prefilter(reader_stats.rows_skipped_prefilter);

  stats_state::S3Stats* s3_stats_result = execution_stats->mutable_s3();
  s3_stats_result->set_s3_requests(s3_stats.requests);
  s3_stats_result->set_bytes_read_from_s3(s3_stats.bytes_read);
  s3_stats_result->set_retry_count(s3_stats.retry_count);

  stats_state::EqualityDeleteStats* eqdel_stats = execution_stats->mutable_equality_delete();
  eqdel_stats->set_files_read(equality_delete_stats.files_read);
  eqdel_stats->set_rows_read(equality_delete_stats.rows_read);
  eqdel_stats->set_max_rows_materialized(equality_delete_stats.max_rows_materialized);

  stats_state::PositionalDeleteStats* posdel_stats = execution_stats->mutable_positional_delete();
  posdel_stats->set_files_read(positional_delete_stats.files_read);
  posdel_stats->set_rows_read(positional_delete_stats.rows_read);
  posdel_stats->set_rows_ignored(reader_stats.positional_delete_rows_ignored);

  stats_state::PlanStats* plan_stats = execution_stats->mutable_plan();
  plan_stats->set_data_files_planned(planner_stats.data_files_planned);
  plan_stats->set_potisional_files_planned(planner_stats.positional_files_planned);
  plan_stats->set_equality_files_planned(planner_stats.equality_files_planned);
  plan_stats->set_dangling_positional_files(planner_stats.dangling_positional_files);

  stats_state::Durations* durations = execution_stats->mutable_durations();
  SetDuration(duration_ticks, ticks_per_second, durations->mutable_total());
  SetDuration(planner_stats.plan_duration, ticks_per_second, durations->mutable_plan());
  SetDuration(reader_stats.fetch_duration, ticks_per_second, durations->mutable_fetch());
  SetDuration(reader_stats.gandiva_filter_build_duration, ticks_per_second, durations->mutable_filter_build());
  SetDuration(reader_stats.gandiva_filter_apply_duration, ticks_per_second, durations->mutable_filter_apply());
  SetDuration(reader_stats.read_duration, ticks_per_second, durations->mutable_read());
  SetDuration(ext_stats.convert_duration_ticks, ticks_per_second, durations->mutable_convert());
  SetDuration(ext_stats.heap_form_tuple_ticks, ticks_per_second, durations->mutable_heap_form_tuple());
  SetDuration(reader_stats.positional_delete_apply_duration, ticks_per_second, durations->mutable_positional());
  SetDuration(reader_stats.equality_delete_apply_duration, ticks_per_second, durations->mutable_equality());
  SetDuration(planner_stats.iceberg_fs_duration, ticks_per_second, durations->mutable_iceberg_plan_fs());

  stats_state::ProjectionStats* projection = execution_stats->mutable_projection();
  projection->set_columns_read(reader_stats.columns_read);
  projection->set_columns_for_greenplum(reader_stats.columns_for_greenplum);
  projection->set_columns_equality_delete(reader_stats.columns_equality_delete);
  projection->set_columns_only_for_equality_delete(reader_stats.columns_only_for_equality_delete);

  stats_state::SamovarStats* samovar = execution_stats->mutable_samovar();
  samovar->set_samovar_initial_tasks_count(planner_stats.samovar_initial_tasks_count);
  samovar->set_samovar_splitted_tasks_count(planner_stats.samovar_splitted_tasks_count);
  samovar->set_samovar_fetched_tasks_count(reader_stats.samovar_fetched_tasks_count);
  SetDuration(reader_stats.samovar_total_response_duration_ticks, ticks_per_second,
              samovar->mutable_samovar_total_response_duration_ticks());
  samovar->set_samovar_requests_count(reader_stats.samovar_requests_count);
  samovar->set_samovar_errors_count(reader_stats.samovar_errors_count);

  stats_state::HmsStats* hms = execution_stats->mutable_hms();
  hms->set_connections_established(planner_stats.catalog_connections_established);

  return result;
}

std::unique_ptr<stats_state::StatsState::Stub> MakeStatsStateChannel() {
  tea::SignalBlocker signal_blocker;

  auto channel = grpc::CreateChannel("localhost:50003", grpc::InsecureChannelCredentials());
  auto ci = stats_state::StatsState::NewStub(channel);

  return ci;
}

}  // namespace

// used only for testing purposes
void SendStats(DurationTicks duration_ticks, double ticks_per_second, const PlannerStats& planner_stats,
               const ReaderStats& reader_stats, const S3Stats& s3_stats, const ExtStats& ext_stats,
               const iceberg::PositionalDeleteStats& positional_delete_stats,
               const iceberg::EqualityDeleteStats& equality_delete_stats, int segment_id) {
  auto ci = MakeStatsStateChannel();

  stats_state::StatsRequest request =
      StatsToProto(duration_ticks, ticks_per_second, planner_stats, reader_stats, s3_stats, ext_stats,
                   positional_delete_stats, equality_delete_stats, segment_id);
  stats_state::Response response;
  grpc::ClientContext context;
  ci->SetStats(&context, request, &response);
}

void SendGandivaFilter(const std::string& gandiva_filter) {
  auto ci = debug::MakeStatsStateChannel();

  stats_state::GandivaFilterRequest request;
  stats_state::Response response;
  grpc::ClientContext context;
  request.set_gandiva_filter(gandiva_filter);
  ci->SetGandivaFilter(&context, request, &response);
}

void SendPotentialRowGroupFilter(const std::string& filter_string) {
  auto ci = debug::MakeStatsStateChannel();

  stats_state::GandivaFilterRequest request;
  request.set_gandiva_filter(filter_string);
  stats_state::Response response;
  grpc::ClientContext context;
  ci->SetPotentialRowGroupFilter(&context, request, &response);
}

}  // namespace tea::debug
