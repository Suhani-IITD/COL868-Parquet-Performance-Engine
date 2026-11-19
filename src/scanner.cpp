#include "parquetscope/scanner.hpp"
#include "parquetscope/value_digest.hpp"

#include <algorithm>
#include <memory>
#include <limits>
#include <stdexcept>

namespace parquetscope {
namespace {
using DecodedColumns = std::vector<std::span<int64_t>>;

struct ColumnSelection {
    std::vector<size_t> projection_indices;
    size_t predicate_index = 0;
    int64_t lower = std::numeric_limits<int64_t>::min();
    int64_t upper = std::numeric_limits<int64_t>::max();
    bool empty = false;
};

// Check reader identity/counters and output limits before any selected chunk is read.
void validate_scan_input(FileReader& file, const Metadata& metadata, const ScanOptions& options) {
    file.verify_unchanged();
    if (options.include_rows && options.max_output_rows > max_result_rows) {
        throw std::runtime_error("scan: requested output row cap exceeds 100000");
    }
    if (file.size() != metadata.file_size || file.identity() != metadata.reader_identity) {
        throw std::runtime_error("scan: metadata belongs to a different file reader");
    }
    if (options.timing && options.digest) {
        throw std::invalid_argument("scan: verification and timing must run separately");
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
        const auto& predicate = *result.plan.query.predicate;
        selection.predicate_index = find_required_column_index(result.plan, predicate.column);
        // Normalize exclusive bounds without signed overflow at the extrema.
        if (predicate.lower) {
            selection.lower = predicate.lower->value;
            if (!predicate.lower->inclusive) {
                if (selection.lower == std::numeric_limits<int64_t>::max()) { selection.empty = true; }
                else { ++selection.lower; }
            }
        }
        if (predicate.upper) {
            selection.upper = predicate.upper->value;
            if (!predicate.upper->inclusive) {
                if (selection.upper == std::numeric_limits<int64_t>::min()) { selection.empty = true; }
                else { --selection.upper; }
            }
        }
        selection.empty = selection.empty || selection.lower > selection.upper;
    }
    return selection;
}

// Add one chunk's observed counters to scan totals; no per-value history is retained.
void add_decode_counts(DecodeCounts& total, const DecodeCounts& chunk) {
    total.data_pages += chunk.data_pages;
    total.dictionary_pages += chunk.dictionary_pages;
    total.plain_values += chunk.plain_values;
    total.dictionary_values += chunk.dictionary_values;
    total.rle_runs += chunk.rle_runs;
    total.bit_packed_runs += chunk.bit_packed_runs;
    total.rle_values += chunk.rle_values;
    total.bit_packed_values += chunk.bit_packed_values;
    total.dictionary_entries += chunk.dictionary_entries;
    total.checksums_verified += chunk.checksums_verified;
    for (size_t width = 0; width < total.bit_width_pages.size(); ++width) {
        total.bit_width_pages[width] += chunk.bit_width_pages[width];
    }
}

// Read/decode all required columns of one retained group and return their value buffers.
DecodedColumns read_group_columns(FileReader& file, const Metadata& metadata,
                                   const GroupPlan& group, const ScanOptions& options, ScanContext& context, ScanResult& result) {
    if (group.chunks.empty()) {
        throw std::runtime_error("scan: retained group has no required chunks");
    }
    const auto row_count = static_cast<uint64_t>(group.rows);
    if (row_count > max_decoded_group_bytes / sizeof(int64_t) / group.chunks.size()) {
        throw std::runtime_error("scan: decoded row group exceeds 256 MiB resource limit");
    }
    const bool timing = options.timing;
    auto allocation_start = start_timer(timing);
    context.columns.resize(group.chunks.size());
    // Retained capacities are bounded even when queries/group sizes change.
    uint64_t retained_values = 0;
    for (const auto& buffer : context.columns) {
        retained_values += std::max<uint64_t>(row_count, buffer.capacity());
    }
    if (retained_values > max_decoded_group_bytes / sizeof(int64_t)) {
        context.columns.clear();
        context.columns.resize(group.chunks.size());
    }
    DecodedColumns columns;
    columns.reserve(group.chunks.size());
    add_elapsed(timing, allocation_start, result.timing.allocation_ms);
    for (size_t column = 0; column < group.chunks.size(); ++column) {
        const auto& range = group.chunks[column];
        if (range.bytes > max_chunk_bytes) {
            throw std::runtime_error("scan: raw chunk exceeds 64 MiB resource limit");
        }
        // Allocate uninitialized storage only on growth, outside the read timer.
        auto started = start_timer(timing);
        auto bytes = context.raw.prepare(static_cast<size_t>(range.bytes));
        columns.push_back(context.columns[column].prepare(static_cast<size_t>(row_count)));
        add_elapsed(timing, started, result.timing.allocation_ms);

        started = start_timer(timing);
        file.read_into(range.offset, bytes);
        add_elapsed(timing, started, result.timing.read_ms);

        started = start_timer(timing);
        try {
            const auto& chunk = metadata.row_groups[group.row_group].columns[range.column_index];
            DecodeCounts decoded;
            auto decoding = options.decoding;
            decoding.timing = nullptr;
            if (options.timing && options.diagnostics) { decoding.timing = &result.timing; }
            decode_chunk(bytes, chunk, columns.back(), decoded, decoding);
            add_decode_counts(result.decoded, decoded);
            if (options.diagnostics) { result.chunks.push_back({group.row_group, range.column, decoded}); }
        } catch (const std::exception& error) {
            throw std::runtime_error("row group " + std::to_string(group.row_group) +
                                     ", column " + range.column + ": " + error.what());
        }
        add_elapsed(timing, started, result.timing.decode_ms);
    }
    return columns;
}

// Test normalized signed bounds using unsigned subtraction (defined at INT64 extrema).
bool matches_interval(int64_t value, const ColumnSelection& selection) {
    return uint64_t(value) - uint64_t(selection.lower) <=
           uint64_t(selection.upper) - uint64_t(selection.lower);
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
    if (selection.empty) {
        add_elapsed(options.timing, started, result.timing.filter_ms);
        return;
    }
    // Count-only scans avoid optional-bound, digest and output-mode branches per row.
    if (!digest && !options.include_rows) {
        if (!predicate) {
            result.matched_rows += columns.front().size();
        } else {
            uint64_t matches = 0;
            for (const auto value : columns[selection.predicate_index]) {
                matches += matches_interval(value, selection);
            }
            result.matched_rows += matches;
        }
        add_elapsed(options.timing, started, result.timing.filter_ms);
        return;
    }
    for (size_t row = 0; row < columns.front().size(); ++row) {
        if (predicate && !matches_interval(columns[selection.predicate_index][row], selection)) {
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
    result.timed = options.timing;
    ScanContext local_context;
    auto* context = options.context;
    if (!context) { context = &local_context; }
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
        const auto columns = read_group_columns(file, metadata, group, options, *context, result);
        filter_group_rows(columns, selection, options, digest.get(), digest_row, result_cells, result);
        // The views expire here; context storage is overwritten by the next group.
    }
    finish_scan_io(file, initial_bytes, initial_calls, result);
    add_elapsed(options.timing, execution_start, result.timing.execution_ms);
    if (digest) {
        result.row_digest = digest->finish();
        result.verification = digest->summaries();
        result.verification.resize(selection.projection_indices.size());
    }
    return result;
}
} // namespace parquetscope
