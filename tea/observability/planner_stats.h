#pragma once

#include <cstdint>

#include "tea/util/measure.h"

namespace tea {

struct PlannerStats {
  int64_t samovar_initial_tasks_count = 0;
  int64_t samovar_splitted_tasks_count = 0;

  int64_t iceberg_bytes_read = 0;
  int64_t iceberg_requests = 0;
  int64_t iceberg_files_read = 0;

  int64_t data_files_planned = 0;
  int64_t positional_files_planned = 0;
  int64_t equality_files_planned = 0;

  int64_t dangling_positional_files = 0;

  int64_t catalog_connections_established = 0;

  DurationTicks iceberg_fs_duration = 0;
  DurationTicks plan_duration = 0;

  void Combine(const PlannerStats& other) {
    samovar_initial_tasks_count += other.samovar_initial_tasks_count;
    samovar_splitted_tasks_count += other.samovar_splitted_tasks_count;

    iceberg_bytes_read += other.iceberg_bytes_read;
    iceberg_requests += other.iceberg_requests;
    iceberg_files_read += other.iceberg_files_read;

    iceberg_fs_duration += other.iceberg_fs_duration;
    plan_duration += other.plan_duration;

    data_files_planned += other.data_files_planned;
    positional_files_planned += other.positional_files_planned;
    equality_files_planned += other.equality_files_planned;

    dangling_positional_files += other.dangling_positional_files;

    catalog_connections_established += other.catalog_connections_established;
  }
};

}  // namespace tea
