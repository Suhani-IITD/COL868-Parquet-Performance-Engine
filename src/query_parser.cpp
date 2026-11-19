#include "parquetscope/query_parser.hpp"

#include <charconv>
#include <chrono>
#include <limits>
#include <set>
#include <stdexcept>

namespace parquetscope {
namespace {

// Split comma-separated column names into a vector; reject empty names.
std::vector<std::string> parse_projection_columns(const std::string& text) {
    std::vector<std::string> columns;
    size_t start = 0;
    while (true) {
        const size_t end = text.find(',', start);
        size_t length = std::string::npos;
        if (end != std::string::npos) {
            length = end - start;
        }
        const auto column = text.substr(start, length);
        if (column.empty()) {
            throw std::invalid_argument("empty projection column");
        }
        columns.push_back(column);
        if (end == std::string::npos) {
            return columns;
        }
        start = end + 1;
    }
}

// Consume the token after a flag, advancing index; reject a missing value.
std::string read_flag_value(int argc, char* argv[], int& index, const std::string& flag) {
    ++index;
    if (index == argc) {
        throw std::invalid_argument("missing value for " + flag);
    }
    std::string value = argv[index];
    if (value.empty() || value.starts_with("--")) {
        throw std::invalid_argument("missing value for " + flag);
    }
    return value;
}

// Read one known flag and its value, updating the raw query arguments.
void parse_query_flag(int argc, char* argv[], int& index, const std::string& flag,
                      QueryCommand command, QueryArguments& arguments) {
    if (flag == "--json") {
        arguments.json = true;
    } else if (flag == "--no-prune") {
        arguments.pruning = false;
    } else if (flag == "--rows" && command == QueryCommand::Scan) {
        arguments.rows = true;
    } else if (flag == "--select") {
        arguments.projection = parse_projection_columns(read_flag_value(argc, argv, index, flag));
    } else if (flag == "--where") {
        arguments.predicate_column = read_flag_value(argc, argv, index, flag);
    } else if (flag == "--model" && command == QueryCommand::Predict) {
        arguments.model = read_flag_value(argc, argv, index, flag);
    } else if (flag == "--eq") {
        arguments.equal = read_flag_value(argc, argv, index, flag);
    } else if (flag == "--ge" || flag == "--gt") {
        const auto value = read_flag_value(argc, argv, index, flag);
        if (arguments.lower) {
            throw std::invalid_argument("multiple lower endpoints");
        }
        arguments.lower = value;
        arguments.lower_inclusive = flag == "--ge";
    } else if (flag == "--le" || flag == "--lt") {
        const auto value = read_flag_value(argc, argv, index, flag);
        if (arguments.upper) {
            throw std::invalid_argument("multiple upper endpoints");
        }
        arguments.upper = value;
        arguments.upper_inclusive = flag == "--le";
    } else {
        throw std::invalid_argument("unknown option: " + flag);
    }
}

// Check required fields and incompatible bounds; throw on invalid combinations.
void validate_query_arguments(const QueryArguments& arguments, QueryCommand command) {
    if (command == QueryCommand::Predict && !arguments.model) {
        throw std::invalid_argument("--model is required");
    }
    if (arguments.projection.empty()) {
        throw std::invalid_argument("--select is required");
    }
    if (arguments.equal && (arguments.lower || arguments.upper)) {
        throw std::invalid_argument("--eq cannot be combined with range endpoints");
    }
    const bool has_endpoints = arguments.equal || arguments.lower || arguments.upper;
    if (has_endpoints != arguments.predicate_column.has_value()) {
        throw std::invalid_argument("--where requires endpoints, and endpoints require --where");
    }
}

// Convert the entire input text to INT64, rejecting overflow or trailing characters.
int64_t parse_signed_integer(std::string_view text) {
    if (text.empty()) {
        throw std::invalid_argument("empty literal");
    }
    if (text.front() == '+') {
        text.remove_prefix(1);
        if (text.empty() || text.front() < '0' || text.front() > '9') {
            throw std::invalid_argument("invalid signed integer literal");
        }
    }
    int64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument("invalid or overflowing signed integer literal");
    }
    return value;
}

// Convert a YYYY-MM-DD string to epoch days, rejecting invalid calendar dates.
int64_t parse_date_days(std::string_view text) {
    for (size_t index = 0; index < text.size(); ++index) {
        if (index != 4 && index != 7 && (text[index] < '0' || text[index] > '9')) {
            throw std::invalid_argument("invalid ISO DATE literal");
        }
    }
    const auto year = static_cast<int>(parse_signed_integer(text.substr(0, 4)));
    const auto month = static_cast<unsigned>(parse_signed_integer(text.substr(5, 2)));
    const auto day = static_cast<unsigned>(parse_signed_integer(text.substr(8, 2)));
    const std::chrono::year_month_day date{
        std::chrono::year{year}, std::chrono::month{month}, std::chrono::day{day}};
    if (year < 1 || !date.ok()) {
        throw std::invalid_argument("invalid Gregorian DATE literal");
    }
    return std::chrono::sys_days{date}.time_since_epoch().count();
}

} // namespace

// Parse COMMAND FILE flags into text fields; reject duplicate or conflicting flags.
QueryArguments parse_query_arguments(int argc, char* argv[], QueryCommand command) {
    QueryArguments arguments;
    // Repeating --select, --eq, etc. is an error rather than silently overwriting it.
    std::set<std::string> provided_flags;
    for (int index = 3; index < argc; ++index) {
        const std::string flag = argv[index];
        if (!provided_flags.insert(flag).second) {
            throw std::invalid_argument("duplicate option: " + flag);
        }
        parse_query_flag(argc, argv, index, flag, command, arguments);
    }
    validate_query_arguments(arguments, command);
    return arguments;
}

// Interpret query text using the selected column and return its numeric value.
int64_t parse_predicate_value(const Metadata& metadata, const std::string& column,
                              std::string_view text) {
    const auto& schema = *find_column(metadata, column).node;
    // This converts query text, not values from data pages or metadata statistics.
    const bool iso_date = schema.date && text.size() == 10 && text[4] == '-' && text[7] == '-';
    int64_t value;
    if (iso_date) {
        value = parse_date_days(text);
    } else {
        value = parse_signed_integer(text);
    }
    if (schema.physical_type == "INT32" &&
        (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max())) {
        throw std::invalid_argument("literal outside column's signed physical range");
    }
    return value;
}

// Build typed query bounds after schema loading; the raw arguments stay unchanged.
Query convert_query_values(const QueryArguments& arguments, const Metadata& metadata) {
    Query query;
    query.projection = arguments.projection;
    query.pruning = arguments.pruning;
    if (!arguments.predicate_column) {
        return query;
    }
    Predicate predicate;
    predicate.column = *arguments.predicate_column;
    if (arguments.equal) {
        const auto value = parse_predicate_value(metadata, predicate.column, *arguments.equal);
        predicate.lower = Endpoint{value, true};
        predicate.upper = Endpoint{value, true};
    } else {
        if (arguments.lower) {
            const auto value = parse_predicate_value(metadata, predicate.column, *arguments.lower);
            predicate.lower = Endpoint{value, arguments.lower_inclusive};
        }
        if (arguments.upper) {
            const auto value = parse_predicate_value(metadata, predicate.column, *arguments.upper);
            predicate.upper = Endpoint{value, arguments.upper_inclusive};
        }
    }
    query.predicate = predicate;
    return query;
}

} // namespace parquetscope
