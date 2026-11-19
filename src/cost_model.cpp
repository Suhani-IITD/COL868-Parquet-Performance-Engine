#include "parquetscope/cost_model.hpp"
#include "parquetscope/metadata_reader.hpp"
#include "parquetscope/scanner.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <limits>
#include <spawn.h>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>
#include <vector>

extern char** environ;

namespace parquetscope {
namespace {
using Matrix = std::vector<std::vector<double>>;

constexpr std::array<std::pair<const char*, double CostModel::*>, 18> model_fields{{
    {"fixed_ms", &CostModel::fixed_ms},
    {"planning_start_ms", &CostModel::planning_start_ms},
    {"footer_byte_ms", &CostModel::footer_byte_ms},
    {"group_col_ms", &CostModel::group_col_ms},
    {"read_start_ms", &CostModel::read_start_ms},
    {"chunk_ms", &CostModel::chunk_ms},
    {"byte_ms", &CostModel::byte_ms},
    {"raw_buffer_byte_ms", &CostModel::raw_buffer_byte_ms},
    {"decode_start_ms", &CostModel::decode_start_ms},
    {"plain_value_ms", &CostModel::plain_value_ms},
    {"packed_value_ms", &CostModel::packed_value_ms},
    {"packed_bit_ms", &CostModel::packed_bit_ms},
    {"packed_dict_mb_ms", &CostModel::packed_dict_mb_ms},
    {"rle_value_ms", &CostModel::rle_value_ms},
    {"output_buffer_byte_ms", &CostModel::output_buffer_byte_ms},
    {"dictionary_entry_ms", &CostModel::dictionary_entry_ms},
    {"dictionary_chunk_ms", &CostModel::dictionary_chunk_ms},
    {"filter_row_ms", &CostModel::filter_row_ms},
}};

// Split a chunk's values by how they decode, using only sizes from the footer. Bit-packed IDs
// take bits / 8 bytes each and RLE runs almost none, so with d dictionary values:
//   data bytes > d * bits / 8  ->  the writer fell back to PLAIN for the excess;
//   data bytes < d * bits / 8  ->  the shortfall was absorbed by RLE runs.
void add_chunk_features(const Chunk& chunk, CostFeatures& features) {
    const auto values = static_cast<double>(chunk.values.value_or(0));
    if (!chunk.dictionary_offset) {
        features.plain_values += values;
        return;
    }
    const uint64_t width = chunk.physical_type == "INT32" ? 4 : 8;
    // Thrift dictionary page headers take roughly 12 bytes; subtract this so entry count is not inflated.
    constexpr uint64_t estimated_dict_header_bytes = 12;
    const auto raw_dict_bytes = static_cast<uint64_t>(*chunk.data_offset - *chunk.dictionary_offset);
    const auto dictionary_bytes = raw_dict_bytes > estimated_dict_header_bytes
        ? raw_dict_bytes - estimated_dict_header_bytes
        : raw_dict_bytes;
    const auto entries = std::max<uint64_t>(1, dictionary_bytes / width);
    const auto bits = static_cast<double>(std::bit_width(entries - 1));
    const auto data_bytes = static_cast<double>(static_cast<uint64_t>(*chunk.compressed_size) - raw_dict_bytes);
    const double packed_bytes = values * bits / 8;
    double plain = 0, packed = 0;
    if (data_bytes >= packed_bytes) {
        const double saving = static_cast<double>(width) - bits / 8;
        plain = saving > 0 ? std::clamp((data_bytes - packed_bytes) / saving, 0.0, values) : 0.0;
        packed = values - plain;
    } else if (bits > 0) {
        // Accounting for minimum RLE header overhead (~2 bytes per run, averaging ~10 values per run)
        const double rle_byte_rate = std::max(0.1, std::ceil(bits / 8.0) / 10.0);
        const double packed_byte_rate = bits / 8.0;
        if (packed_byte_rate > rle_byte_rate) {
            packed = std::clamp((data_bytes - values * rle_byte_rate) / (packed_byte_rate - rle_byte_rate), 0.0, values);
        } else {
            packed = std::clamp(data_bytes * 8 / bits, 0.0, values);
        }
    }
    const double rle = std::max(0.0, values - plain - packed);
    features.plain_values += plain;
    features.packed_values += packed;
    features.packed_bits += packed * bits;
    features.packed_dict_mb += packed * static_cast<double>(dictionary_bytes) / 1e6;
    features.rle_values += rle;
    features.dictionary_entries += static_cast<double>(entries);
    features.dictionary_chunks += 1.0;
}

// Gaussian elimination with partial pivoting; false when the system is singular.
bool solve(Matrix a, std::vector<double> b, std::vector<double>& x) {
    const size_t n = b.size();
    for (size_t col = 0; col < n; ++col) {
        size_t pivot = col;
        for (size_t row = col + 1; row < n; ++row) {
            if (std::abs(a[row][col]) > std::abs(a[pivot][col])) { pivot = row; }
        }
        if (std::abs(a[pivot][col]) < 1e-12) { return false; }
        std::swap(a[col], a[pivot]);
        std::swap(b[col], b[pivot]);
        for (size_t row = col + 1; row < n; ++row) {
            const double factor = a[row][col] / a[col][col];
            for (size_t k = col; k < n; ++k) { a[row][k] -= factor * a[col][k]; }
            b[row] -= factor * b[col];
        }
    }
    x.assign(n, 0.0);
    for (size_t row = n; row-- > 0;) {
        double sum = b[row];
        for (size_t k = row + 1; k < n; ++k) { sum -= a[row][k] * x[k]; }
        x[row] = sum / a[row][row];
    }
    return true;
}

// Non-negative least squares on relative error. Exhaustive over active sets, which is exact
// and cheap for the one to three terms of each stage.
std::vector<double> fit_nonnegative(const Matrix& x, const std::vector<double>& y) {
    const size_t terms = x.empty() ? 0 : x.front().size();
    std::vector<double> scale(terms, 1e-12), best(terms, 0.0);
    for (const auto& row : x) {
        for (size_t j = 0; j < terms; ++j) { scale[j] = std::max(scale[j], std::abs(row[j])); }
    }
    const auto weight = [&](size_t i) { return 1.0 / std::max(y[i], 0.05); };
    double best_error = std::numeric_limits<double>::infinity();
    for (size_t mask = 1; mask < (size_t{1} << terms); ++mask) {
        std::vector<size_t> active;
        for (size_t j = 0; j < terms; ++j) {
            if ((mask >> j) & 1) { active.push_back(j); }
        }
        const size_t n = active.size();
        Matrix a(n, std::vector<double>(n, 0.0));
        std::vector<double> b(n, 0.0), solution;
        for (size_t i = 0; i < x.size(); ++i) {
            const double w2 = weight(i) * weight(i);
            for (size_t r = 0; r < n; ++r) {
                const double xr = x[i][active[r]] / scale[active[r]];
                b[r] += w2 * xr * y[i];
                for (size_t c = 0; c < n; ++c) { a[r][c] += w2 * xr * x[i][active[c]] / scale[active[c]]; }
            }
        }
        if (!solve(a, b, solution) || std::any_of(solution.begin(), solution.end(), [](double v) { return v < 0; })) {
            continue;
        }
        std::vector<double> coefficients(terms, 0.0);
        for (size_t r = 0; r < n; ++r) { coefficients[active[r]] = solution[r] / scale[active[r]]; }
        double error = 0;
        for (size_t i = 0; i < x.size(); ++i) {
            double predicted = 0;
            for (size_t j = 0; j < terms; ++j) { predicted += coefficients[j] * x[i][j]; }
            error += std::pow((predicted - y[i]) * weight(i), 2);
        }
        if (error < best_error) {
            best_error = error;
            best = std::move(coefficients);
        }
    }
    return best;
}

// Return the median value from a set of timing samples.
double median(std::vector<double> values) {
    const auto middle = values.begin() + static_cast<std::ptrdiff_t>(values.size() / 2);
    std::nth_element(values.begin(), middle, values.end());
    return *middle;
}

// Run this executable as a child process and parse its JSON output.
nlohmann::json run_self(std::vector<std::string> args) {
    int fds[2];
    if (pipe(fds) != 0) { throw std::runtime_error("calibrate: pipe failed"); }
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_adddup2(&actions, fds[1], STDOUT_FILENO);
    posix_spawn_file_actions_addclose(&actions, fds[0]);
    args.insert(args.begin(), "/proc/self/exe");
    std::vector<char*> argv;
    for (auto& arg : args) { argv.push_back(arg.data()); }
    argv.push_back(nullptr);
    pid_t pid = 0;
    const int spawned = posix_spawn(&pid, argv[0], &actions, nullptr, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    close(fds[1]);
    std::string output;
    char buffer[4096];
    for (ssize_t n; spawned == 0 && (n = read(fds[0], buffer, sizeof buffer)) > 0;) {
        output.append(buffer, static_cast<size_t>(n));
    }
    close(fds[0]);
    int status = 0;
    if (spawned != 0 || waitpid(pid, &status, 0) != pid || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        throw std::runtime_error("calibrate: scan subprocess failed");
    }
    return nlohmann::json::parse(output);
}

struct Sample {
    CostFeatures features;
    ScanTiming timing;
};

// Median stage times of repeated CLI scans after a warm-up. Separate processes include the
// start-up and first-touch costs that every real invocation pays.
Sample measure(const std::string& path, std::vector<std::string> args, CostFeatures features, int repeats) {
    args.insert(args.begin(), {"scan", path});
    args.push_back("--timing");
    run_self(args);
    std::vector<nlohmann::json> runs;
    for (int run = 0; run < repeats; ++run) { runs.push_back(run_self(args).at("timing_ms")); }
    const auto stage = [&](const char* name) {
        std::vector<double> values;
        for (const auto& timing : runs) { values.push_back(timing.at(name).get<double>()); }
        return median(std::move(values));
    };
    Sample sample{features, {}};
    sample.timing.planning_ms = stage("planning");
    sample.timing.read_ms = stage("read");
    sample.timing.decode_ms = stage("decode");
    sample.timing.filter_ms = stage("filter");
    sample.timing.execution_ms = stage("execution");
    return sample;
}
} // namespace

// Extract metadata-derived query cost features without reading any data pages.
CostFeatures extract_features(const Metadata& metadata, const Plan& plan) {
    CostFeatures features;
    double largest_group_rows = 0;
    features.footer_bytes = static_cast<double>(metadata.footer_length);
    features.groups_columns = static_cast<double>(plan.groups.size() * plan.required_columns.size());
    for (const auto& group : plan.groups) {
        if (!group.retained) { continue; }
        ++features.groups;
        largest_group_rows = std::max(largest_group_rows, static_cast<double>(group.rows));
        if (plan.query.predicate) { features.predicate_rows += static_cast<double>(group.rows); }
        for (const auto& chunk : group.chunks) {
            ++features.chunks;
            features.bytes += static_cast<double>(chunk.bytes);
            features.raw_buffer_bytes = std::max(features.raw_buffer_bytes, static_cast<double>(chunk.bytes));
            add_chunk_features(metadata.row_groups[group.row_group].columns[chunk.column_index], features);
        }
    }
    features.output_buffer_bytes =
        largest_group_rows * static_cast<double>(plan.required_columns.size() * sizeof(int64_t));
    return features;
}

// Predict planning, read, decode, filter, and overhead latency from features.
CostPrediction predict_cost(const CostModel& model, const CostFeatures& features) {
    CostPrediction prediction;
    // Start-up terms apply once a stage runs at all; a fully pruned query reads nothing.
    const double runs = features.chunks > 0 ? 1.0 : 0.0;
    prediction.planning_ms = runs * model.planning_start_ms + model.footer_byte_ms * features.footer_bytes +
        model.group_col_ms * features.groups_columns;
    prediction.read_ms = runs * model.read_start_ms + model.byte_ms * features.bytes +
        model.chunk_ms * features.chunks + model.raw_buffer_byte_ms * features.raw_buffer_bytes;
    prediction.decode_ms = runs * model.decode_start_ms + model.plain_value_ms * features.plain_values +
        model.packed_value_ms * features.packed_values + model.packed_bit_ms * features.packed_bits +
        model.packed_dict_mb_ms * features.packed_dict_mb + model.rle_value_ms * features.rle_values +
        model.output_buffer_byte_ms * features.output_buffer_bytes +
        model.dictionary_entry_ms * features.dictionary_entries +
        model.dictionary_chunk_ms * features.dictionary_chunks;
    prediction.filter_ms = model.filter_row_ms * features.predicate_rows;
    prediction.overhead_ms = model.fixed_ms;
    prediction.total_ms = prediction.planning_ms + prediction.read_ms + prediction.decode_ms +
        prediction.filter_ms + prediction.overhead_ms;
    return prediction;
}

// Scan every supported column whole, with and without an always-true predicate, plus all columns
// together; then fit each stage separately.
CostModel calibrate(std::span<const std::string> paths, int repeats) {
    std::vector<Sample> samples;
    for (const auto& path : paths) {
        const auto metadata = inspect(path);
        Query all{{}, std::nullopt, false};
        for (const auto& node : metadata.schema) {
            if (node.path.size() != 1 || !node.flat_required || !node.signed_interpretation ||
                (node.physical_type != "INT32" && node.physical_type != "INT64")) {
                continue;
            }
            const int64_t minimum = node.physical_type == "INT32" ? int64_t{std::numeric_limits<int32_t>::min()}
                                                                  : std::numeric_limits<int64_t>::min();
            Query whole{{node.name}, std::nullopt, false};
            Query filtered = whole;
            filtered.predicate = Predicate{node.name, Endpoint{minimum, true}, std::nullopt};
            const std::vector<std::string> args{"--select", node.name, "--no-prune"};
            const std::vector<std::string> filter_args{"--select", node.name, "--where", node.name,
                                                       "--ge", std::to_string(minimum), "--no-prune"};
            samples.push_back(measure(path, args, extract_features(metadata, make_plan(metadata, whole)), repeats));
            samples.push_back(
                measure(path, filter_args, extract_features(metadata, make_plan(metadata, filtered)), repeats));
            all.projection.push_back(node.name);
        }
        if (all.projection.empty()) { continue; }
        // One wide scan per file gives decode times well above timer noise.
        std::string columns;
        for (const auto& name : all.projection) { columns += (columns.empty() ? "" : ",") + name; }
        samples.push_back(measure(path, {"--select", columns, "--no-prune"},
                                  extract_features(metadata, make_plan(metadata, all)), repeats));
    }
    if (samples.empty()) { throw std::invalid_argument("calibration files have no scannable columns"); }

    Matrix planning_x, read_x, decode_x, filter_x, fixed_x;
    std::vector<double> planning_y, read_y, decode_y, filter_y, fixed_y;
    for (const auto& [f, t] : samples) {
        planning_x.push_back({1.0, f.footer_bytes, f.groups_columns});
        planning_y.push_back(t.planning_ms);
        read_x.push_back({1.0, f.bytes, f.chunks, f.raw_buffer_bytes});
        read_y.push_back(t.read_ms);
        decode_x.push_back({1.0, f.plain_values, f.packed_values, f.packed_bits, f.packed_dict_mb, f.rle_values,
                            f.output_buffer_bytes, f.dictionary_entries, f.dictionary_chunks});
        decode_y.push_back(t.decode_ms);
        if (f.predicate_rows > 0) {
            filter_x.push_back({f.predicate_rows});
            filter_y.push_back(t.filter_ms);
        }
        fixed_x.push_back({1.0});
        fixed_y.push_back(std::max(0.0, t.execution_ms - t.read_ms - t.decode_ms - t.filter_ms));
    }
    const auto planning = fit_nonnegative(planning_x, planning_y);
    const auto read = fit_nonnegative(read_x, read_y);
    const auto decode = fit_nonnegative(decode_x, decode_y);
    const auto filter = fit_nonnegative(filter_x, filter_y);
    CostModel model;
    model.planning_start_ms = planning[0];
    model.footer_byte_ms = planning[1];
    model.group_col_ms = planning[2];
    model.read_start_ms = read[0];
    model.byte_ms = read[1];
    model.chunk_ms = read[2];
    model.raw_buffer_byte_ms = read[3];
    model.decode_start_ms = decode[0];
    model.plain_value_ms = decode[1];
    model.packed_value_ms = decode[2];
    model.packed_bit_ms = decode[3];
    model.packed_dict_mb_ms = decode[4];
    model.rle_value_ms = decode[5];
    model.output_buffer_byte_ms = decode[6];
    model.dictionary_entry_ms = decode[7];
    model.dictionary_chunk_ms = decode[8];
    model.filter_row_ms = filter.empty() ? 0.0 : filter[0];
    model.fixed_ms = fit_nonnegative(fixed_x, fixed_y)[0];
    return model;
}

// Serialize cost model coefficients to JSON.
nlohmann::json model_to_json(const CostModel& model) {
    nlohmann::json json;
    for (const auto& [name, field] : model_fields) { json[name] = model.*field; }
    return json;
}

// Deserialize cost model coefficients from JSON.
CostModel model_from_json(const nlohmann::json& json) {
    CostModel model;
    for (const auto& [name, field] : model_fields) { model.*field = json.at(name).get<double>(); }
    return model;
}

// Format cost prediction and feature breakdown into structured JSON output.
nlohmann::json prediction_to_json(const Plan& plan, const CostFeatures& features,
                                  const CostPrediction& prediction) {
    return {
        {"kind", "cost_prediction"},
        {"retained_groups", plan.retained_groups},
        {"planned_chunk_bytes", plan.planned_chunk_bytes},
        {"features", {{"groups", features.groups}, {"chunks", features.chunks}, {"bytes", features.bytes},
                      {"predicate_rows", features.predicate_rows}, {"plain_values", features.plain_values},
                      {"packed_values", features.packed_values}, {"packed_bits", features.packed_bits},
                      {"packed_dict_mb", features.packed_dict_mb}, {"rle_values", features.rle_values},
                      {"dictionary_entries", features.dictionary_entries},
                      {"dictionary_chunks", features.dictionary_chunks},
                      {"raw_buffer_bytes", features.raw_buffer_bytes},
                      {"output_buffer_bytes", features.output_buffer_bytes},
                      {"footer_bytes", features.footer_bytes},
                      {"groups_columns", features.groups_columns}}},
        {"predicted_ms", {{"planning", prediction.planning_ms}, {"read", prediction.read_ms},
                          {"decode", prediction.decode_ms}, {"filter", prediction.filter_ms},
                          {"overhead", prediction.overhead_ms}, {"total", prediction.total_ms}}},
        {"note", "Metadata-only estimate; no data pages were read."}};
}

} // namespace parquetscope
