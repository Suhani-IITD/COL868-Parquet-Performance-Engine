#pragma once

#include "parquetscope/metadata.hpp"
#include <string_view>

namespace parquetscope {

struct Endpoint {
    int64_t value;
    bool inclusive = true;
};
struct Predicate {
    std::string column;
    std::optional<Endpoint> lower, upper;
};
struct Query {
    std::vector<std::string> projection;
    std::optional<Predicate> predicate;
    bool pruning = true;
};

// Borrows a schema node: metadata must outlive this lookup result.
struct ColumnReference {
    size_t index;
    const SchemaNode* node;
};
ColumnReference find_column(const Metadata& metadata, const std::string& name);

struct PlannedChunk {
    size_t column_index;
    std::string column;
    uint64_t offset, bytes;
};
struct GroupPlan {
    size_t row_group;
    int64_t rows;
    bool retained = true;
    std::string reason;
    std::optional<int64_t> min, max;
    std::vector<PlannedChunk> chunks;
    uint64_t chunk_bytes = 0;
};
struct Plan {
    Query query;
    std::string predicate_unit;
    std::vector<std::string> required_columns;
    std::vector<GroupPlan> groups;
    uint64_t metadata_bytes = 0, metadata_read_calls = 0, planned_chunk_bytes = 0;
    uint64_t candidate_rows = 0;
    size_t retained_groups = 0;
};

// Pure metadata operation. Retained rows are candidates, not confirmed matches.
Plan make_plan(const Metadata& metadata, const Query& query);

} // namespace parquetscope
