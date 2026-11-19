#pragma once
#include "parquetscope/decoder.hpp"
#include "parquetscope/planner.hpp"
#include "parquetscope/timing.hpp"

namespace parquetscope {
inline constexpr uint64_t max_decoded_group_bytes = 256 * 1024 * 1024;
inline constexpr size_t max_result_cells = 1000000;
inline constexpr size_t max_result_rows = 100000;

struct ScanOptions {
    bool include_rows = false;
    size_t max_output_rows = max_result_rows;
    bool timing = false;
    bool digest = false;
};
struct ResultColumn {
    std::string name, physical_type, unit;
};
struct ScanResult {
    Plan plan;
    ScanTiming timing;
    std::optional<std::string> row_digest;
    uint64_t matched_rows = 0;
    uint64_t chunk_read_bytes = 0, chunk_read_calls = 0;
    DecodeCounts decoded;
    std::vector<ResultColumn> columns;
    bool rows_included = false;
    std::vector<std::vector<int64_t>> rows;
};

// Scan using fresh metadata from the same unchanged reader; return counts/optional rows.
// Selected chunks must validate before a result is returned. Planning occurs inside scan.
ScanResult scan(FileReader& file, const Metadata& metadata, const Query& query,
                ScanOptions options = {});
} // namespace parquetscope
