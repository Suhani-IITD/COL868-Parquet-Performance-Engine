#pragma once
#include "parquetscope/decoder.hpp"
#include "parquetscope/planner.hpp"
#include "parquetscope/timing.hpp"
#include "parquetscope/reusable_buffer.hpp"
#include "parquetscope/value_digest.hpp"

namespace parquetscope {
inline constexpr uint64_t max_decoded_group_bytes = 256 * 1024 * 1024;
inline constexpr size_t max_result_cells = 1000000;
inline constexpr size_t max_result_rows = 100000;

// Caller-owned workspace; retain across scans for warm-buffer experiments.
// Buffers contain no cached query results. A context must not be used concurrently.
struct ScanContext {
    ReusableBuffer<uint8_t> raw;
    std::vector<ReusableBuffer<int64_t>> columns;
    ReusableBuffer<uint8_t> predicate_mask;
    bool mask_valid = false;
};
struct ScanOptions {
    bool include_rows = false;
    size_t max_output_rows = max_result_rows;
    bool timing = false;
    bool digest = false;
    bool diagnostics = false;
    DecodeOptions decoding;
    ScanContext* context = nullptr;
};
struct ResultColumn {
    std::string name, physical_type, unit;
};
struct ChunkDiagnostics {
    size_t row_group = 0;
    std::string column;
    DecodeCounts decoded;
};
struct ScanResult {
    Plan plan;
    ScanTiming timing;
    std::optional<std::string> row_digest;
    std::vector<ValueSummary> verification;
    std::vector<ChunkDiagnostics> chunks;
    bool timed = false;
    uint64_t matched_rows = 0;
    uint64_t chunk_read_bytes = 0, chunk_read_calls = 0;
    DecodeCounts decoded;
    std::vector<ResultColumn> columns;
    bool rows_included = false;
    std::vector<std::vector<int64_t>> rows;
};

// Scan using metadata from the same unchanged reader; repeated scans are supported.
// Selected chunks must validate before a result is returned. Planning occurs inside scan.
ScanResult scan(FileReader& file, const Metadata& metadata, const Query& query,
                ScanOptions options = {});
} // namespace parquetscope
