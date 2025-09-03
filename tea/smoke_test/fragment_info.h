#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#define LOGLEVEL_21 LOGLEVEL_INFO
#include "teapot/teapot.grpc.pb.h"

namespace tea {

struct PositionalDeleteInfo {
  std::string path;
};

struct EqualityDeleteInfo {
  std::string path;
  std::vector<int32_t> field_ids;
};

struct FragmentInfo {
  std::string data_path;
  std::vector<PositionalDeleteInfo> pos_deletes;
  std::vector<EqualityDeleteInfo> eq_deletes_info;
  std::optional<int64_t> position;
  std::optional<int64_t> length;

  explicit FragmentInfo(std::string str) : data_path(std::move(str)) {}

  FragmentInfo GetCopy() const { return *this; }

  FragmentInfo SetPosition(int64_t position_other) && {
    position = position_other;
    return std::move(*this);
  }

  FragmentInfo SetFromTo(int64_t from, int64_t to) && {
    position = from;
    length = to - from;
    return std::move(*this);
  }

  FragmentInfo SetLength(int64_t length_other) && {
    length = length_other;
    return std::move(*this);
  }

  FragmentInfo AddPositionalDelete(std::string path) && {
    pos_deletes.emplace_back(PositionalDeleteInfo{std::move(path)});
    return std::move(*this);
  }

  FragmentInfo AddEqualityDelete(std::string path, std::vector<int32_t> field_ids) && {
    eq_deletes_info.emplace_back(EqualityDeleteInfo{std::move(path), std::move(field_ids)});
    return std::move(*this);
  }
};

teapot::MetadataResponse TeapotExpectedResponse(const std::vector<FragmentInfo>& fragments_info);

}  // namespace tea
