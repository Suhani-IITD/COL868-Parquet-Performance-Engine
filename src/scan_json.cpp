#include "parquetscope/scan_format.hpp"

namespace parquetscope {
using json = nlohmann::json;
namespace {

// Return the aggregate counters from decoding selected chunks.
json decode_counts_to_json(const DecodeCounts& counts) {
    return {{"data_pages", counts.data_pages}, {"dictionary_pages", counts.dictionary_pages},
        {"plain_values", counts.plain_values}, {"dictionary_values", counts.dictionary_values},
        {"rle_runs", counts.rle_runs}, {"bit_packed_runs", counts.bit_packed_runs},
        {"checksums_verified", counts.checksums_verified}};
}

// Return metadata, planned chunk and actual chunk read accounting.
json scan_io_to_json(const ScanResult& result) {
    return {{"metadata_bytes", result.plan.metadata_bytes},
        {"metadata_read_calls", result.plan.metadata_read_calls},
        {"planned_chunk_bytes", result.plan.planned_chunk_bytes},
        {"actual_chunk_bytes", result.chunk_read_bytes}, {"chunk_read_calls", result.chunk_read_calls}};
}
} // namespace

// Convert an already completed scan to JSON; formatting performs no file reads.
json scan_to_json(const ScanResult& result) {
    json row_digest = nullptr;
    if (result.row_digest) {
        row_digest = *result.row_digest;
    }
    json rows = nullptr;
    if (result.rows_included) {
        rows = result.rows;
    }
    std::string validation = "selected_chunks_validated";
    if (result.plan.retained_groups == 0) {
        validation = "no_chunks_read";
    }
    json output = {
        {"kind", "scan_result"}, {"matched_rows", result.matched_rows}, {"row_digest", row_digest},
        {"candidate_rows", result.plan.candidate_rows}, {"scanned_groups", result.plan.retained_groups},
        {"pruned_groups", result.plan.groups.size() - result.plan.retained_groups},
        {"columns", json::array()}, {"rows", rows}, {"page_validation", validation},
        {"io", scan_io_to_json(result)}, {"decoded", decode_counts_to_json(result.decoded)},
        {"note", "Only selected chunks were validated; pruning assumes truthful metadata. Application bytes are not physical disk I/O."}};
    for (const auto& column : result.columns) {
        output["columns"].push_back({{"name", column.name}, {"physical_type", column.physical_type},
                                    {"unit", column.unit}});
    }
    return output;
}
} // namespace parquetscope
