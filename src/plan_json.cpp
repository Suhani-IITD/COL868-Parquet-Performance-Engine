#include "parquetscope/plan_format.hpp"

namespace parquetscope {
using json = nlohmann::json;
namespace {

// Serialize an optional numeric endpoint, using JSON null when absent.
json endpoint_to_json(const std::optional<Endpoint>& endpoint) {
    if (!endpoint) {
        return nullptr;
    }
    return {{"value", endpoint->value}, {"inclusive", endpoint->inclusive}};
}

// Serialize the plan predicate and its value unit, or null for a full scan.
json predicate_to_json(const Plan& plan) {
    if (!plan.query.predicate) {
        return nullptr;
    }
    const auto& predicate = *plan.query.predicate;
    return {{"column", predicate.column}, {"lower", endpoint_to_json(predicate.lower)},
            {"upper", endpoint_to_json(predicate.upper)}, {"unit", plan.predicate_unit}};
}

// Serialize one row-group decision and the selected chunk ranges.
json group_plan_to_json(const GroupPlan& group) {
    json minimum = nullptr;
    json maximum = nullptr;
    if (group.min) {
        minimum = *group.min;
    }
    if (group.max) {
        maximum = *group.max;
    }
    json result = {
        {"row_group", group.row_group}, {"rows", group.rows}, {"retained", group.retained},
        {"reason", group.reason},
        {"min", minimum},
        {"max", maximum},
        {"planned_chunk_bytes", group.chunk_bytes}, {"chunks", json::array()}};
    for (const auto& chunk : group.chunks) {
        result["chunks"].push_back({{"column_index", chunk.column_index},
            {"column", chunk.column}, {"offset", chunk.offset}, {"bytes", chunk.bytes}});
    }
    return result;
}

} // namespace

// Serialize a metadata-only plan without performing any data reads.
json plan_to_json(const Plan& plan) {
    json result = {
        {"kind", "metadata_only_plan"}, {"page_compatibility", "unverified"},
        {"projection", plan.query.projection}, {"predicate", predicate_to_json(plan)},
        {"statistics_pruning", plan.query.pruning}, {"required_columns", plan.required_columns},
        {"row_groups", json::array()}, {"retained_groups", plan.retained_groups},
        {"pruned_groups", plan.groups.size() - plan.retained_groups},
        {"candidate_rows", plan.candidate_rows}, {"planned_chunk_bytes", plan.planned_chunk_bytes},
        {"metadata_io", {{"application_bytes", plan.metadata_bytes}, {"read_calls", plan.metadata_read_calls}}},
        {"note", "Provisional metadata-only plan: page compatibility unverified; no query executed. Candidate rows are not matching rows. Bytes are application bytes, not physical disk I/O."}};
    for (const auto& group : plan.groups) {
        result["row_groups"].push_back(group_plan_to_json(group));
    }
    return result;
}

} // namespace parquetscope
