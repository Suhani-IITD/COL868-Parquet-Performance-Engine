#pragma once
#include "parquetscope/planner.hpp"
#include <nlohmann/json.hpp>

namespace parquetscope {
// Return a JSON description of pruning decisions and planned byte ranges.
nlohmann::json plan_to_json(const Plan& plan);
} // namespace parquetscope
