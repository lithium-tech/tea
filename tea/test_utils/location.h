#pragma once

#include <optional>
#include <string>
#include <variant>

namespace tea {

struct Options {
  std::string profile;
  std::optional<int64_t> snapshot_id;
  std::optional<std::string> branch;
};

struct LocationBase {
  explicit LocationBase(const Options& options_) : options(options_) {}

  std::string ApplyOptions(const std::string& result) const {
    std::string opt_str = result;
    bool has_query = opt_str.find('?') != std::string::npos;
    auto append_param = [&](const std::string& key, const std::string& val) {
      if (has_query) {
        opt_str += "&" + key + "=" + val;
      } else {
        opt_str += "?" + key + "=" + val;
        has_query = true;
      }
    };
    if (!options.profile.empty()) {
      append_param("profile", options.profile);
    }
    if (options.snapshot_id.has_value()) {
      append_param("snapshot_id", std::to_string(*options.snapshot_id));
    }
    if (options.branch.has_value()) {
      append_param("branch", *options.branch);
    }
    return opt_str;
  }

  Options options;
};

#if 0
struct LocalFileLocation : public {
  std::string ToString() const { return ApplyOptions("tea://file://"); }

  std::string path;
};
#endif

struct TeapotLocation : public LocationBase {
  explicit TeapotLocation(const std::string& db_, const std::string& table_name_, const std::string& host_, int port_,
                          const Options& options_)
      : LocationBase(options_), db(db_), table_name(table_name_), host(host_), port(port_) {}

  std::string ToString() const {
    return ApplyOptions("tea://teapot://" + host + ":" + std::to_string(port) + "/" + db + "." + table_name);
  }

  std::string db;
  std::string table_name;
  std::string host;
  int port;
};

struct IcebergLocation : public LocationBase {
  explicit IcebergLocation(const std::string& hms_db_name_, const std::string& hms_table_name_, const Options& options)
      : LocationBase(options), hms_db_name(hms_db_name_), hms_table_name(hms_table_name_) {}

  std::string ToString() const {
    if (LocationBase::options.profile.empty()) {
      std::string res = "tea://" + hms_db_name + "." + hms_table_name + "?profile=iceberg_table";
      if (LocationBase::options.snapshot_id.has_value()) {
        res += "&snapshot_id=" + std::to_string(*LocationBase::options.snapshot_id);
      }
      if (LocationBase::options.branch.has_value()) {
        res += "&branch=" + *LocationBase::options.branch;
      }
      return res;
    }
    return ApplyOptions("tea://iceberg://" + hms_db_name + "." + hms_table_name);
  }

  std::string hms_db_name;
  std::string hms_table_name;
};

struct SimpleLocation : public LocationBase {
  explicit SimpleLocation(const std::string& db_name_, const std::string& table_name_, const Options& options = {})
      : LocationBase(options), db_name(db_name_), table_name(table_name_) {}

  std::string ToString() const { return ApplyOptions("tea://" + db_name + "." + table_name); }

  std::string db_name;
  std::string table_name;
};

struct SpecialLocation {
  explicit SpecialLocation(const std::string& name) : name(name) {}

  std::string ToString() const { return "tea://special://" + name; }

  std::string name;
};

struct Location {
  template <typename SomeLocation>
  explicit Location(const SomeLocation& other_location) : location(other_location) {}

  Location(const Location& other_location) = default;

  std::string ToString() const {
    return std::visit([](auto&& arg) { return arg.ToString(); }, location);
  }

  std::variant<TeapotLocation, IcebergLocation, SimpleLocation, SpecialLocation> location;
};

}  // namespace tea
