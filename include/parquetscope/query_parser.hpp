#pragma once

#include "parquetscope/planner.hpp"

namespace parquetscope {

// Raw command-line values; predicate endpoints have not yet been typed by schema.
struct QueryArguments {
    std::vector<std::string> projection;
    std::optional<std::string> predicate_column;
    std::optional<std::string> lower, upper, equal, model;
    bool lower_inclusive = true;
    bool upper_inclusive = true;
    bool pruning = true;
    bool json = false;
    bool rows = false;
    bool verify = false, timing = false, diagnostics = false;
    bool naive_decoder = false, separate_ids = false;
};

enum class QueryCommand { Plan, Scan, Predict };

// Read flags after COMMAND FILE and return validated text arguments.
QueryArguments parse_query_arguments(int argc, char* argv[],
                                    QueryCommand command = QueryCommand::Plan);
// Convert one query value to a signed integer or epoch days using its column type.
int64_t parse_predicate_value(const Metadata& metadata, const std::string& column,
                              std::string_view text);
// Convert raw endpoint strings to typed bounds and return a complete Query.
Query convert_query_values(const QueryArguments& arguments, const Metadata& metadata);

} // namespace parquetscope
