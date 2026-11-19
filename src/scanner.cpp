#include "parquetscope/scanner.hpp"
#include "parquetscope/value_digest.hpp"

#include <algorithm>
#include <memory>
#include <stdexcept>

namespace parquetscope {
namespace {
using DecodedColumns = std::vector<std::vector<int64_t>>;

struct ColumnSelection {
    std::vector<size_t> projection_indices;
    size_t predicate_index = 0;
};

// Check reader identity/counters and output limits before any selected chunk is read.
void validate_scan_input(FileReader& file, const Metadata& metadata, const ScanOptions& options) {
    file.verify_unchanged();
    if (options.include_rows && options.max_output_rows > max_result_rows) {
        throw std::runtime_error("scan: requested output row cap exceeds 100000");
    }
    if (file.size() != metadata.file_size || file.bytes_read() != metadata.read_bytes ||
        file.read_calls() != metadata.read_calls) {
        throw std::runtime_error("scan: use fresh metadata from the same file reader");
    }
}

// Find a column's position within decoded group buffers, whose order follows the plan.
size_t find_required_column_index(const Plan& plan, const std::string& name) {
    const auto position = std::find(plan.required_columns.begin(), plan.required_columns.end(), name);
    if (position == plan.required_columns.end()) {
        throw std::runtime_error("scan: column absent from required columns");
    }
    return static_cast<size_t>(position - plan.required_columns.begin());
}

// Bind projection/predicate names to buffer indices and populate result column descriptions.
ColumnSelection prepare_column_selection(const Metadata& metadata, ScanResult& result) {
    ColumnSelection selection;
    for (const auto& name : result.plan.query.projection) {
        selection.projection_indices.push_back(find_required_column_index(result.plan, name));
        const auto node = std::find_if(metadata.schema.begin(), metadata.schema.end(), [&](const auto& entry) {
            return entry.path.size() == 1 && entry.path.front() == name;
        });
        if (node == metadata.schema.end()) {
            throw std::runtime_error("scan: projection schema unavailable");
        }
        std::string unit = "signed_integer";
        if (node->date) {
            unit = "epoch_days";
        }
        result.columns.push_back({name, node->physical_type, unit});
    }
    if (result.plan.query.predicate) {
        selection.predicate_index = find_required_column_index(result.plan, result.plan.query.predicate->column);
    }
    return selection;
}

// Read/decode all required columns of one retained group and return their value buffers.
DecodedColumns read_group_columns(FileReader& file, const Metadata& metadata,
                                   const GroupPlan& group, bool timing, ScanResult& result) {
    if (group.chunks.empty()) {
        throw std::runtime_error("scan: retained group has no required chunks");
    }
    const auto row_count = static_cast<uint64_t>(group.rows);
    if (row_count > max_decoded_group_bytes / sizeof(int64_t) / group.chunks.size()) {
        throw std::runtime_error("scan: decoded row group exceeds 256 MiB resource limit");
    }
    DecodedColumns columns;
    columns.reserve(group.chunks.size());
    for (const auto& range : group.chunks) {
        if (range.bytes > max_chunk_bytes) {
            throw std::runtime_error("scan: raw chunk exceeds 64 MiB resource limit");
        }
        // Raw bytes live only through this column's decode.
        auto started = start_timer(timing);
        const auto bytes = file.read(range.offset, range.bytes);
        add_elapsed(timing, started, result.timing.read_ms);

        started = start_timer(timing);
        columns.emplace_back(static_cast<size_t>(row_count));
        add_elapsed(timing, started, result.timing.allocation_ms);

        started = start_timer(timing);
        try {
            const auto& chunk = metadata.row_groups[group.row_group].columns[range.column_index];
            decode_chunk(bytes, chunk, columns.back(), result.decoded);
        } catch (const std::exception& error) {
            throw std::runtime_error("row group " + std::to_string(group.row_group) +
                                     ", column " + range.column + ": " + error.what());
        }
        add_elapsed(timing, started, result.timing.decode_ms);
    }
    return columns;
}

// Return whether a decoded value satisfies both optional query endpoints.
bool matches_predicate(int64_t value, const Predicate& predicate) {
    if (predicate.lower) {
        if (value < predicate.lower->value) {
            return false;
        }
        if (value == predicate.lower->value && !predicate.lower->inclusive) {
            return false;
        }
    }
    if (predicate.upper) {
        if (value > predicate.upper->value) {
            return false;
        }
        if (value == predicate.upper->value && !predicate.upper->inclusive) {
            return false;
        }
    }
    return true;
}

// Append one matched row in projection order, enforcing the total output limits.
void append_result_row(const DecodedColumns& columns, size_t row,
                       const ColumnSelection& selection, const ScanOptions& options,
                       size_t& result_cells, ScanResult& result) {
    if (result.rows.size() >= options.max_output_rows) {
        throw std::runtime_error("scan: row output limit exceeded; use count-only output");
    }
    if (selection.projection_indices.size() > max_result_cells - result_cells) {
        throw std::runtime_error("scan: row output exceeds 1000000 cells");
    }
    std::vector<int64_t> projected;
    projected.reserve(selection.projection_indices.size());
    for (auto index : selection.projection_indices) {
        projected.push_back(columns[index][row]);
    }
    result.rows.push_back(std::move(projected));
    result_cells += selection.projection_indices.size();
}

// Count matches and optionally hash/materialize projected rows from this decoded group.
void filter_group_rows(const DecodedColumns& columns, const ColumnSelection& selection,
                       const ScanOptions& options, ValueDigest* digest,
                       std::vector<int64_t>& digest_row, size_t& result_cells, ScanResult& result) {
    const auto& predicate = result.plan.query.predicate;
    const auto started = start_timer(options.timing);
    for (size_t row = 0; row < columns.front().size(); ++row) {
        if (predicate && !matches_predicate(columns[selection.predicate_index][row], *predicate)) {
            continue;
        }
        ++result.matched_rows;
        if (digest) {
            digest_row.clear();
            for (auto index : selection.projection_indices) {
                digest_row.push_back(columns[index][row]);
            }
            digest->row(digest_row);
        }
        if (options.include_rows) {
            append_result_row(columns, row, selection, options, result_cells, result);
        }
    }
    add_elapsed(options.timing, started, result.timing.filter_ms);
}

// Recheck the file and record actual chunk I/O, which must equal the whole-chunk plan.
void finish_scan_io(FileReader& file, uint64_t initial_bytes, uint64_t initial_calls, ScanResult& result) {
    file.verify_unchanged();
    result.chunk_read_bytes = file.bytes_read() - initial_bytes;
    result.chunk_read_calls = file.read_calls() - initial_calls;
    if (result.chunk_read_bytes != result.plan.planned_chunk_bytes) {
        throw std::runtime_error("scan: actual chunk bytes differ from plan");
    }
}
} // namespace

// Build a plan, decode retained groups one at a time, and return fully validated results.
ScanResult scan(FileReader& file, const Metadata& metadata, const Query& query, ScanOptions options) {
    validate_scan_input(file, metadata, options);
    ScanResult result;
    std::unique_ptr<ValueDigest> digest;
    if (options.digest) {
        digest = std::make_unique<ValueDigest>();
    }
    std::vector<int64_t> digest_row;

    // Planning needs metadata only. Execution timing begins after this stage.
    const auto planning_start = start_timer(options.timing);
    result.plan = make_plan(metadata, query);
    add_elapsed(options.timing, planning_start, result.timing.planning_ms);
    const auto execution_start = start_timer(options.timing);
    result.rows_included = options.include_rows;
    const auto selection = prepare_column_selection(metadata, result);
    const auto initial_bytes = file.bytes_read();
    const auto initial_calls = file.read_calls();
    size_t result_cells = 0;

    for (const auto& group : result.plan.groups) {
        if (!group.retained) {
            continue;
        }
        const auto columns = read_group_columns(file, metadata, group, options.timing, result);
        filter_group_rows(columns, selection, options, digest.get(), digest_row, result_cells, result);
        // Decoded columns are released here before processing the next group.
    }
    finish_scan_io(file, initial_bytes, initial_calls, result);
    add_elapsed(options.timing, execution_start, result.timing.execution_ms);
    if (digest) {
        result.row_digest = digest->finish();
    }
    return result;
}
} // namespace parquetscope
