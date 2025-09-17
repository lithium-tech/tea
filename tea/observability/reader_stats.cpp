#include "tea/observability/reader_stats.h"

#include "rapidjson/encodings.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "tea/observability/ext_stats.h"
#include "tea/observability/s3_stats.h"
#include "tea/util/measure.h"

namespace tea {

namespace {

using JsonStringBuffer = rapidjson::GenericStringBuffer<rapidjson::ASCII<>>;
using JsonWriter = rapidjson::Writer<JsonStringBuffer, rapidjson::ASCII<>>;

void WriteString(JsonWriter& writer, std::string_view key, std::string_view value) {
  writer.Key(key.data(), key.size());
  writer.String(value.data(), value.size());
}

void WriteDouble(JsonWriter& writer, std::string_view key, double value) {
  writer.Key(key.data(), key.size());
  writer.Double(value);
}

void WriteUInt64(JsonWriter& writer, std::string_view key, uint64_t value) {
  writer.Key(key.data(), key.size());
  writer.Uint64(value);
}

double ToSeconds(DurationTicks duration, double ticks_per_second) { return duration / ticks_per_second; }

void WriteReaderStats(JsonWriter& writer, const ReaderStats& stats, double ticks_per_second) {
  WriteDouble(
      writer, "fetch_duration_seconds",
      ToSeconds(stats.fetch_duration - stats.gandiva_filter_build_duration - stats.gandiva_filter_apply_duration -
                    stats.read_duration - stats.positional_delete_apply_duration - stats.equality_delete_apply_duration,
                ticks_per_second));
  WriteDouble(writer, "filter_build_seconds", ToSeconds(stats.gandiva_filter_build_duration, ticks_per_second));
  WriteDouble(writer, "filter_apply_seconds", ToSeconds(stats.gandiva_filter_apply_duration, ticks_per_second));
  WriteDouble(writer, "read_duration_seconds", ToSeconds(stats.read_duration, ticks_per_second));
  WriteDouble(writer, "positional_delete_apply_seconds",
              ToSeconds(stats.positional_delete_apply_duration, ticks_per_second));
  WriteDouble(writer, "equality_delete_apply_seconds",
              ToSeconds(stats.equality_delete_apply_duration, ticks_per_second));
  WriteUInt64(writer, "data_files_planned", stats.data_files_planned);
  WriteUInt64(writer, "data_files_read", stats.data_files_read);
  WriteUInt64(writer, "row_groups_read", stats.row_groups_read);
  WriteUInt64(writer, "row_groups_skipped_filter", stats.row_groups_skipped_filter);
  WriteUInt64(writer, "rows_read", stats.rows_read);
  WriteUInt64(writer, "rows_skipped_filter", stats.rows_skipped_filter);
  WriteUInt64(writer, "rows_skipped_prefilter", stats.rows_skipped_prefilter);
  WriteUInt64(writer, "rows_skipped_equality_delete", stats.rows_skipped_equality_delete);
  WriteUInt64(writer, "rows_skipped_positional_delete", stats.rows_skipped_positional_delete);
  WriteUInt64(writer, "positional_delete_rows_ignored", stats.positional_delete_rows_ignored);
  WriteUInt64(writer, "columns_for_greenplum", stats.columns_for_greenplum);
  WriteUInt64(writer, "columns_equality_delete", stats.columns_equality_delete);
  WriteUInt64(writer, "columns_only_for_equality_delete", stats.columns_only_for_equality_delete);
  WriteUInt64(writer, "columns_read", stats.columns_read);
  WriteUInt64(writer, "samovar_fetched_tasks_count", stats.samovar_fetched_tasks_count);
  WriteUInt64(writer, "samovar_requests_count", stats.samovar_requests_count);
  WriteUInt64(writer, "samovar_errors_count", stats.samovar_errors_count);
  WriteDouble(writer, "samovar_response_seconds",
              ToSeconds(stats.samovar_total_response_duration_ticks, ticks_per_second));
}

void WritePlannerStats(JsonWriter& writer, const PlannerStats& stats, double ticks_per_second) {
  WriteUInt64(writer, "samovar_splitted_tasks_count", stats.samovar_splitted_tasks_count);
  WriteUInt64(writer, "samovar_initial_tasks_count", stats.samovar_initial_tasks_count);
  WriteDouble(writer, "plan_duration_seconds", ToSeconds(stats.plan_duration, ticks_per_second));
  WriteDouble(writer, "iceberg_fs_duration_seconds", ToSeconds(stats.iceberg_fs_duration, ticks_per_second));
  WriteUInt64(writer, "iceberg_bytes_read", stats.iceberg_bytes_read);
  WriteUInt64(writer, "iceberg_files_read", stats.iceberg_files_read);
  WriteUInt64(writer, "iceberg_fs_requests", stats.iceberg_requests);
  WriteUInt64(writer, "catalog_connections_established", stats.catalog_connections_established);
}

void WritePositionalDeleteStats(JsonWriter& writer, const iceberg::PositionalDeleteStats& stats) {
  WriteUInt64(writer, "positional_delete_files_read", stats.files_read);
  WriteUInt64(writer, "positional_delete_rows_read", stats.rows_read);
}

void WriteEqualityDeleteStats(JsonWriter& writer, const iceberg::EqualityDeleteStats& stats) {
  WriteUInt64(writer, "equality_delete_files_read", stats.files_read);
  WriteUInt64(writer, "equality_delete_rows_read", stats.rows_read);
  WriteUInt64(writer, "equality_delete_max_rows_materialized", stats.max_rows_materialized);
  WriteDouble(writer, "equality_delete_max_mb_size_materialized", stats.max_mb_size_materialized);
}

void WriteS3Stats(JsonWriter& writer, const S3Stats& stats) {
  WriteUInt64(writer, "retry_count", stats.retry_count);
  WriteUInt64(writer, "bytes_read_from_s3", stats.bytes_read);
  WriteUInt64(writer, "s3_requests", stats.requests);
}

void WriteExtStats(JsonWriter& writer, const ExtStats& stats, double ticks_per_second) {
  WriteDouble(writer, "heap_form_tuple_seconds", ToSeconds(stats.heap_form_tuple_ticks, ticks_per_second));
  WriteDouble(writer, "convert_duration_seconds", ToSeconds(stats.convert_duration_ticks, ticks_per_second));
}
}  // namespace

std::string FormatStats(std::string_view event_type, std::string_view session_id, uint64_t scan_identifier,
                        std::string_view version, DurationTicks total_duration_ticks, double ticks_per_second,
                        const PlannerStats& planner_stats, const ReaderStats& reader_stats, const S3Stats& s3_stats,
                        const ExtStats& ext_stats, const iceberg::PositionalDeleteStats& positional_delete_stats,
                        const iceberg::EqualityDeleteStats& equality_delete_stats, DurationTicks prefetch_duration,
                        DurationTicks wait_duration) {
  JsonStringBuffer buffer;
  JsonWriter writer(buffer);
  writer.StartObject();
  WriteString(writer, "event_type", event_type);
  WriteString(writer, "session_id", session_id);
  WriteUInt64(writer, "scan_id", scan_identifier);
  WriteString(writer, "version", version);
  WriteDouble(writer, "total_duration_seconds", ToSeconds(total_duration_ticks, ticks_per_second));
  WriteUInt64(writer, "total_files_read",
              reader_stats.data_files_read + positional_delete_stats.files_read + equality_delete_stats.files_read);
  WriteDouble(writer, "wait_duration_seconds", ToSeconds(wait_duration, ticks_per_second));
  WriteDouble(writer, "prefetch_duration_seconds", ToSeconds(prefetch_duration, ticks_per_second));
  WritePlannerStats(writer, planner_stats, ticks_per_second);
  WriteReaderStats(writer, reader_stats, ticks_per_second);
  WriteExtStats(writer, ext_stats, ticks_per_second);
  WritePositionalDeleteStats(writer, positional_delete_stats);
  WriteEqualityDeleteStats(writer, equality_delete_stats);
  WriteS3Stats(writer, s3_stats);
  writer.EndObject();

  return {buffer.GetString(), buffer.GetLength()};
}
}  // namespace tea
