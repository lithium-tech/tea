#pragma once

#include <list>
#include <string>

#define TEA_LOG(message) tea::Log(__FILE__, __LINE__, (message))

namespace tea {

void Log(const std::string& file_name, int line_number, const std::string& message);
void Log(const std::string& message);

std::list<std::string> GetLogs();

void InitializeLogger();
void FinalizeLogger();

void InitializeLogger();
void FinalizeLogger();

}  // namespace tea
