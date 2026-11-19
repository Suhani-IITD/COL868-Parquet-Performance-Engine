#pragma once

#include "parquetscope/planner.hpp"
#include <nlohmann/json.hpp>
#include <span>
#include <string>

namespace parquetscope {

// Workload of a plan derived from metadata only; no data pages are read.
struct CostFeatures {
    double groups = 0, chunks = 0, bytes = 0, predicate_rows = 0;
    // Values decoded as PLAIN, as bit-packed dictionary IDs (with their ID bits and the size of
    // the dictionary they index, in MB) or as RLE runs; estimated from chunk sizes.
    double plain_values = 0, packed_values = 0, packed_bits = 0, packed_dict_mb = 0, rle_values = 0;
    // Scan buffers are reused across groups, so a scan first-touches only the largest ones.
    double raw_buffer_bytes = 0, output_buffer_bytes = 0;
};

// Milliseconds per unit of each feature.
struct CostModel {
    double fixed_ms = 0;
    double read_start_ms = 0, chunk_ms = 0, byte_ms = 0, raw_buffer_byte_ms = 0;
    double decode_start_ms = 0, plain_value_ms = 0, packed_value_ms = 0, packed_bit_ms = 0;
    double packed_dict_mb_ms = 0, rle_value_ms = 0, output_buffer_byte_ms = 0;
    double filter_row_ms = 0;
};

struct CostPrediction {
    double read_ms = 0, decode_ms = 0, filter_ms = 0, overhead_ms = 0, total_ms = 0;
};

CostFeatures extract_features(const Metadata& metadata, const Plan& plan);
CostPrediction predict_cost(const CostModel& model, const CostFeatures& features);
// Fit a model from timed whole-column CLI scans of small calibration files.
CostModel calibrate(std::span<const std::string> paths, int repeats);

nlohmann::json model_to_json(const CostModel& model);
CostModel model_from_json(const nlohmann::json& json);
nlohmann::json prediction_to_json(const Plan& plan, const CostFeatures& features,
                                  const CostPrediction& prediction);

} // namespace parquetscope
