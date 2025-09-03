#pragma once

#include <string>
#include <string_view>

namespace tea {

bool NeedToSubstituteAsciiCp1251(std::string_view str);
std::string SubstituteAsciiCp1251(std::string data);

}  // namespace tea
