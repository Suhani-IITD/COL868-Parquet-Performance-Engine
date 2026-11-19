#include "parquetscope/planner.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace parquetscope {

// Find a supported top-level column by name; return its leaf index and borrowed schema.
ColumnReference find_column(const Metadata& metadata, const std::string& name) {
    size_t column_index = 0;
    for (const auto& node : metadata.schema) {
        if (node.physical_type == "GROUP") {
            continue;
        }
        if (node.path.size() == 1 && node.path.front() == name) {
            if (!node.flat_required || !node.signed_interpretation ||
                (node.physical_type != "INT32" && node.physical_type != "INT64")) {
                throw std::invalid_argument(
                    "requested column is outside flat required INT32/INT64/DATE subset: " + name);
            }
            return {column_index, &node};
        }
        ++column_index;
    }
    throw std::invalid_argument("unknown or non-flat column: " + name);
}

namespace {

// Return the smallest/largest signed values supported by this physical type.
std::pair<int64_t, int64_t> physical_value_limits(const SchemaNode& node) {
    if (node.physical_type == "INT32") {
        return {std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
    }
    return {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()};
}

// Check an already numeric query endpoint against the column width.
void validate_endpoint_value(int64_t value, const SchemaNode& node) {
    const auto [minimum, maximum] = physical_value_limits(node);
    if (value < minimum || value > maximum) {
        throw std::invalid_argument("literal outside column's signed physical range");
    }
}

// Return whether the endpoint combination excludes every representable integer.
bool predicate_is_empty(const Predicate& predicate, const SchemaNode& node) {
    const auto& lower = predicate.lower;
    const auto& upper = predicate.upper;
    const auto [minimum, maximum] = physical_value_limits(node);
    if (lower && !lower->inclusive && lower->value == maximum) {
        return true;
    }
    if (upper && !upper->inclusive && upper->value == minimum) {
        return true;
    }
    if (!lower || !upper) {
        return false;
    }
    if (lower->value > upper->value) {
        return true;
    }
    if (lower->value == upper->value) {
        return !lower->inclusive || !upper->inclusive;
    }
    // Adjacent exclusive integer bounds contain no values. Unsigned subtraction
    // gives the distance without overflowing when signed endpoints cross zero.
    return !lower->inclusive && !upper->inclusive &&
        uint64_t(upper->value) - uint64_t(lower->value) == 1;
}

// Add a nonnegative byte/row count, rejecting integer overflow.
void add_to_total(uint64_t& total, uint64_t amount) {
    if (amount > std::numeric_limits<uint64_t>::max() - total) {
        throw std::invalid_argument("plan byte/row total overflow");
    }
    total += amount;
}

// Return the chunk at a bound column index, checking its schema path.
const Chunk& find_group_chunk(const RowGroup& group, const ColumnReference& column) {
    if (column.index >= group.columns.size()) {
        throw std::invalid_argument("row group missing requested column");
    }
    const auto& chunk = group.columns[column.index];
    if (chunk.path != column.node->path) {
        throw std::invalid_argument("requested chunk path differs from schema");
    }
    return chunk;
}

// Update one group decision from usable min/max bounds; uncertain bounds retain it.
void prune_using_statistics(GroupPlan& group, const SchemaNode& node,
                            const Chunk& chunk, const Predicate& predicate) {
    // Unusable statistics retain the group; they never establish no matches.
    if (!chunk.metadata_present || chunk.encrypted || chunk.external_file) {
        group.reason = "unavailable_local_statistics";
        return;
    }
    if (node.column_order != "TYPE_ORDER") {
        group.reason = "unknown_column_order";
        return;
    }
    if (!chunk.statistics) {
        group.reason = "missing_statistics";
        return;
    }
    const auto& statistics = *chunk.statistics;
    if (!statistics.min_value || !statistics.max_value) {
        group.reason = "incomplete_modern_bounds";
        return;
    }
    if (!statistics.min_value->signed_value || !statistics.max_value->signed_value) {
        group.reason = "uninterpretable_bounds";
        return;
    }
    const auto minimum = *statistics.min_value->signed_value;
    const auto maximum = *statistics.max_value->signed_value;
    const auto [domain_minimum, domain_maximum] = physical_value_limits(node);
    if (minimum > maximum || minimum < domain_minimum || maximum > domain_maximum) {
        group.reason = "invalid_bounds";
        return;
    }
    group.min = minimum;
    group.max = maximum;
    const auto& lower = predicate.lower;
    const auto& upper = predicate.upper;
    // Compare the group's largest value with the query's lower endpoint.
    if (lower) {
        if (maximum < lower->value || (maximum == lower->value && !lower->inclusive)) {
            group.retained = false;
            group.reason = "below_predicate";
            return;
        }
    }
    // Compare the group's smallest value with the query's upper endpoint.
    if (upper) {
        if (minimum > upper->value || (minimum == upper->value && !upper->inclusive)) {
            group.retained = false;
            group.reason = "above_predicate";
            return;
        }
    }
    group.retained = true;
    group.reason = "bounds_overlap";
}

// Check that a selected chunk is supported and agrees with schema/group counts.
void validate_chunk_support(const Chunk& chunk, const ColumnReference& column,
                            const RowGroup& group, const Metadata& metadata) {
    const auto prefix = column.node->name + ": ";
    if (!chunk.metadata_present) {
        throw std::invalid_argument(prefix + "column metadata unavailable");
    }
    if (chunk.encrypted || metadata.encrypted) {
        throw std::invalid_argument(prefix + "encrypted data cannot be planned");
    }
    if (chunk.external_file) {
        throw std::invalid_argument(prefix + "external data cannot be planned");
    }
    if (chunk.physical_type != column.node->physical_type) {
        throw std::invalid_argument(prefix + "physical type differs from schema");
    }
    if (chunk.codec != "UNCOMPRESSED") {
        throw std::invalid_argument(prefix + "compression outside initial scanner subset");
    }
    if (chunk.declares_v2) {
        throw std::invalid_argument(prefix + "declared V2 data pages outside initial scanner subset");
    }
    if (chunk.encodings.empty()) {
        throw std::invalid_argument(prefix + "missing declared encodings");
    }
    for (const auto& encoding : chunk.encodings) {
        if (encoding != "PLAIN" && encoding != "PLAIN_DICTIONARY" && encoding != "RLE" &&
            encoding != "BIT_PACKED" && encoding != "RLE_DICTIONARY") {
            throw std::invalid_argument(prefix + "unsupported encoding " + encoding);
        }
    }
    if (!chunk.values || *chunk.values != group.rows) {
        throw std::invalid_argument(prefix + "required value count differs from row count");
    }
}

// Return a checked whole-chunk byte range, including a dictionary when present.
PlannedChunk plan_chunk_range(const Chunk& chunk, const ColumnReference& column,
                             const RowGroup& group, const Metadata& metadata) {
    validate_chunk_support(chunk, column, group, metadata);
    const auto prefix = column.node->name + ": ";
    if (!chunk.data_offset || *chunk.data_offset < 4 ||
        !chunk.compressed_size || *chunk.compressed_size <= 0) {
        throw std::invalid_argument(prefix + "missing or invalid chunk offset/length");
    }
    auto start = *chunk.data_offset;
    // Include dictionary pages in the whole-chunk read.
    if (chunk.dictionary_offset) {
        if (*chunk.dictionary_offset < 4 || *chunk.dictionary_offset > start) {
            throw std::invalid_argument(prefix + "invalid dictionary offset");
        }
        start = *chunk.dictionary_offset;
    }
    const auto offset = static_cast<uint64_t>(start);
    const auto bytes = static_cast<uint64_t>(*chunk.compressed_size);
    if (offset > metadata.footer_offset || bytes > metadata.footer_offset - offset) {
        throw std::invalid_argument(prefix + "chunk range outside data area");
    }
    if (uint64_t(*chunk.data_offset - start) >= bytes) {
        throw std::invalid_argument(prefix + "data offset outside chunk");
    }
    return {column.index, column.node->name, offset, bytes};
}

struct QueryColumns {
    std::vector<ColumnReference> required;
    std::optional<ColumnReference> predicate;
    bool empty_predicate = false;
};

// Bind a unique required column and append its name/index to the plan.
void add_required_column(const Metadata& metadata, const std::string& name,
                         Plan& plan, QueryColumns& columns) {
    // Bind each unique required name once for this planning pass.
    if (std::find(plan.required_columns.begin(), plan.required_columns.end(), name) ==
        plan.required_columns.end()) {
        columns.required.push_back(find_column(metadata, name));
        plan.required_columns.push_back(name);
    }
}

// Bind projection and predicate columns, deduplicate them and validate endpoint semantics.
QueryColumns prepare_query_columns(const Metadata& metadata, const Query& query, Plan& plan) {
    if (query.projection.empty() && !query.count_only) {
        throw std::invalid_argument("at least one projection column is required");
    }
    QueryColumns columns;
    plan.query = query;
    plan.query.projection.clear();
    for (const auto& name : query.projection) {
        add_required_column(metadata, name, plan, columns);
        if (std::find(plan.query.projection.begin(), plan.query.projection.end(), name) ==
            plan.query.projection.end()) {
            plan.query.projection.push_back(name);
        }
    }
    if (query.predicate) {
        const auto& predicate = *query.predicate;
        add_required_column(metadata, predicate.column, plan, columns);
        const auto position = std::find(plan.required_columns.begin(), plan.required_columns.end(), predicate.column);
        columns.predicate = columns.required[static_cast<size_t>(position - plan.required_columns.begin())];
        const auto& node = *columns.predicate->node;
        plan.predicate_unit = "signed_integer";
        if (node.date) {
            plan.predicate_unit = "epoch_days";
        }
        if (!predicate.lower && !predicate.upper) {
            throw std::invalid_argument("predicate needs at least one endpoint");
        }
        if (predicate.lower) {
            validate_endpoint_value(predicate.lower->value, node);
        }
        if (predicate.upper) {
            validate_endpoint_value(predicate.upper->value, node);
        }
        columns.empty_predicate = predicate_is_empty(predicate, node);
    }
    return columns;
}

// Return one group decision and, if retained, its selected chunk ranges.
GroupPlan plan_row_group(const Metadata& metadata, size_t index, const Query& query,
                         const QueryColumns& columns) {
    const auto& source = metadata.row_groups[index];
    if (source.rows < 0) {
        throw std::invalid_argument("negative group row count");
    }
    GroupPlan group{};
    group.row_group = index;
    group.rows = source.rows;
    if (columns.empty_predicate || source.rows == 0) {
        group.retained = false;
        if (columns.empty_predicate) {
            group.reason = "empty_predicate";
        } else {
            group.reason = "empty_row_group";
        }
    } else if (!query.predicate) {
        group.reason = "no_predicate";
    } else if (!query.pruning) {
        group.reason = "pruning_disabled";
    } else if (metadata.encrypted) {
        group.reason = "unavailable_local_statistics";
    } else {
        prune_using_statistics(group, *columns.predicate->node,
                               find_group_chunk(source, *columns.predicate), *query.predicate);
    }
    if (group.retained) {
        if (group.min && group.max && query.predicate) {
            const auto& pred = *query.predicate;
            bool low_ok = !pred.lower || (pred.lower->inclusive ? (*group.min >= pred.lower->value) : (*group.min > pred.lower->value));
            bool up_ok = !pred.upper || (pred.upper->inclusive ? (*group.max <= pred.upper->value) : (*group.max < pred.upper->value));
            if (low_ok && up_ok) {
                group.fully_contained = true;
                group.reason = "fully_contained_within_predicate";
            }
        }
        const bool skip_chunks = query.count_only && (group.fully_contained || !query.predicate);
        if (!skip_chunks) {
            for (const auto& column : columns.required) {
                auto range = plan_chunk_range(find_group_chunk(source, column), column, source, metadata);
                add_to_total(group.chunk_bytes, range.bytes);
                group.chunks.push_back(std::move(range));
            }
        }
    }
    return group;
}

// Check that selected chunk intervals do not overlap; no file reads are made.
void validate_nonoverlapping_ranges(const Plan& plan) {
    std::vector<std::pair<uint64_t, uint64_t>> intervals;
    for (const auto& group : plan.groups) {
        for (const auto& chunk : group.chunks) {
            intervals.emplace_back(chunk.offset, chunk.offset + chunk.bytes);
        }
    }
    std::sort(intervals.begin(), intervals.end());
    for (size_t index = 1; index < intervals.size(); ++index) {
        if (intervals[index].first < intervals[index - 1].second) {
            throw std::invalid_argument("overlapping selected chunk ranges");
        }
    }
}

} // namespace

// Create group decisions and byte totals from metadata and a typed query only.
Plan make_plan(const Metadata& metadata, const Query& query) {
    Plan plan;
    plan.metadata_bytes = metadata.read_bytes;
    plan.metadata_read_calls = metadata.read_calls;
    const auto columns = prepare_query_columns(metadata, query, plan);
    for (size_t index = 0; index < metadata.row_groups.size(); ++index) {
        auto group = plan_row_group(metadata, index, query, columns);
        if (group.retained) {
            ++plan.retained_groups;
            add_to_total(plan.candidate_rows, static_cast<uint64_t>(group.rows));
            add_to_total(plan.planned_chunk_bytes, group.chunk_bytes);
        }
        plan.groups.push_back(std::move(group));
    }
    validate_nonoverlapping_ranges(plan);
    return plan;
}

} // namespace parquetscope
