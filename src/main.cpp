#include "parquetscope/cost_model.hpp"
#include "parquetscope/metadata_reader.hpp"
#include "parquetscope/plan_format.hpp"
#include "parquetscope/query_parser.hpp"
#include "parquetscope/scan_format.hpp"

#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace {
using json = nlohmann::json;
constexpr int calibration_repeats = 5;
constexpr auto usage =
    "Usage: parquet_metadata inspect FILE.parquet\n"
    "       parquet_metadata plan FILE.parquet --select COL[,COL...] [predicate flags]\n"
    "       parquet_metadata scan FILE.parquet --select COL[,COL...] [predicate flags] [--rows]\n"
    "       parquet_metadata predict FILE.parquet --select COL[,COL...] [predicate flags] --model MODEL.json\n"
    "       parquet_metadata calibrate FILE.parquet [FILE.parquet...] > MODEL.json\n"
    "Predicate flags: --where COL (--eq VALUE | [--ge VALUE | --gt VALUE] [--le VALUE | --lt VALUE])\n"
    "Optional: --no-prune. Results are JSON; scan returns a count unless --rows is given.\n"
    "Scan flags: --verify | --timing; --diagnostics; --naive-decoder; --separate-ids.\n"
    "DATE endpoints accept epoch days or YYYY-MM-DD.\n";

// Print one indented JSON result, replacing invalid descriptive UTF-8 for display.
void print_json(const json& result) {
    std::cout << result.dump(2, ' ', false, json::error_handler_t::replace) << '\n';
}

// Return a compact file/schema summary from already parsed metadata.
json summarize_metadata(const parquetscope::Metadata& metadata) {
    json columns = json::array();
    for (const auto& node : metadata.schema) {
        if (node.physical_type != "GROUP") {
            columns.push_back({{"path", node.path}, {"physical_type", node.physical_type},
                               {"logical_type", node.logical_type}});
        }
    }
    return {{"kind", "metadata_summary"}, {"file_bytes", metadata.file_size},
        {"footer_offset", metadata.footer_offset}, {"footer_bytes", metadata.footer_length},
        {"rows", metadata.rows}, {"row_groups", metadata.row_groups.size()},
        {"columns", columns}, {"limitations", metadata.limitations},
        {"metadata_io", {{"application_bytes", metadata.read_bytes}, {"read_calls", metadata.read_calls}}}};
}

// Inspect the supplied file and print its summary; data pages are not read.
void run_inspection(const char* path) {
    print_json(summarize_metadata(parquetscope::inspect(path)));
}

// Convert CLI text using the file schema, then print a plan, cost prediction or completed scan.
void run_query(int argc, char* argv[], parquetscope::QueryCommand command) {
    const auto arguments = parquetscope::parse_query_arguments(argc, argv, command);
    parquetscope::FileReader file(argv[2]);
    const auto metadata = parquetscope::read_metadata(file);
    // A date string or integer endpoint becomes numeric only after its type is known.
    const auto query = parquetscope::convert_query_values(arguments, metadata);
    if (command == parquetscope::QueryCommand::Plan) {
        print_json(parquetscope::plan_to_json(parquetscope::make_plan(metadata, query)));
    } else if (command == parquetscope::QueryCommand::Predict) {
        std::ifstream model_file(*arguments.model);
        if (!model_file) {
            throw std::runtime_error("cannot open model " + *arguments.model);
        }
        const auto model = parquetscope::model_from_json(json::parse(model_file));
        const auto plan = parquetscope::make_plan(metadata, query);
        const auto features = parquetscope::extract_features(metadata, plan);
        print_json(parquetscope::prediction_to_json(plan, features, parquetscope::predict_cost(model, features)));
    } else {
        parquetscope::ScanOptions scan_options;
        scan_options.include_rows = arguments.rows;
        scan_options.digest = arguments.verify;
        scan_options.timing = arguments.timing;
        scan_options.diagnostics = arguments.diagnostics;
        scan_options.decoding.naive_bit_unpacking = arguments.naive_decoder;
        scan_options.decoding.separate_dictionary_ids = arguments.separate_ids;
        print_json(parquetscope::scan_to_json(parquetscope::scan(file, metadata, query, scan_options)));
    }
}

// Dispatch an explicit inspect/plan/scan/predict/calibrate command; return 2 for invalid usage.
int run_command(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << usage;
        return 2;
    }
    const std::string_view command_type = argv[1];
    if (argc == 2 && command_type == "--help") {
        std::cout << usage;
        return 0;
    }
    if (argc < 3) {
        std::cerr << usage;
        return 2;
    }
    if (command_type == "inspect" && argc == 3) {
        run_inspection(argv[2]);
    } else if (command_type == "plan") {
        run_query(argc, argv, parquetscope::QueryCommand::Plan);
    } else if (command_type == "scan") {
        run_query(argc, argv, parquetscope::QueryCommand::Scan);
    } else if (command_type == "predict") {
        run_query(argc, argv, parquetscope::QueryCommand::Predict);
    } else if (command_type == "calibrate") {
        const std::vector<std::string> paths(argv + 2, argv + argc);
        print_json(parquetscope::model_to_json(parquetscope::calibrate(paths, calibration_repeats)));
    } else {
        std::cerr << usage;
        return 2;
    }
    return 0;
}
} // namespace

// Run the command; report query errors as status 2 and file/decode failures as status 1.
int main(int argc, char* argv[]) {
    try {
        return run_command(argc, argv);
    } catch (const std::invalid_argument& error) {
        std::cerr << "Query/usage error: " << error.what() << '\n';
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}
