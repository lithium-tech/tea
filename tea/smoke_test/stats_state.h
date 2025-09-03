#pragma once
#include <memory>
#include <string>
#include <vector>

#include "tea/debug/stats_state.pb.h"

namespace tea {

class StatsState {
 public:
  StatsState();
  ~StatsState();
  StatsState(const StatsState&) = delete;
  StatsState& operator=(const StatsState&) = delete;

  std::string GetLastGandivaFilter() const;
  std::vector<std::string> GetAllGandivaFilters() const;
  std::string GetPotentialRowGroupFilter() const;
  std::string GetHostPort() const;
  void ClearStats();
  void ClearFilters();
  std::vector<stats_state::ExecutionStats> GetStats(bool include_master);

  static int64_t DurationToNanos(::google::protobuf::Duration duration) {
    constexpr int64_t kNanosInSecond = 1'000'000'000;
    return duration.nanos() + kNanosInSecond * duration.seconds();
  }

 private:
  struct StatsStateImpl;
  std::unique_ptr<StatsStateImpl> impl_;
};

}  // namespace tea
