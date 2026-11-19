#pragma once
#include "parquetscope/scanner.hpp"
#include <nlohmann/json.hpp>

namespace parquetscope {
// Serialize a completed scan's counts, optional rows and validation/I/O diagnostics.
nlohmann::json scan_to_json(const ScanResult& result);
} // namespace parquetscope
