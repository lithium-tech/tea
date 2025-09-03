#pragma once

#include <memory>
#include <string>

#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/tea_scan.h"

namespace tea::meta::access {

iceberg::ice_tea::ScanMetadata FromFileUrl(const std::string& file_url,
                                           std::shared_ptr<iceberg::IFileSystemProvider> fs_provider);

}  // namespace tea::meta::access
