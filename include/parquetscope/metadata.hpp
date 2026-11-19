#pragma once

#include "parquetscope/file_reader.hpp"
#include <optional>
#include <string>
#include <vector>

namespace parquetscope {

// One minimum/maximum. Preserve raw bytes as hex even for unsupported types.
struct Bound {
    std::string raw_hex;
    std::optional<int64_t> signed_value;
};

// Missing fields remain absent. Modern and legacy bounds have different provenance.
struct Statistics {
    std::optional<Bound> min_value, max_value, legacy_min, legacy_max;
    std::optional<int64_t> null_count, distinct_count;
    std::optional<bool> min_exact, max_exact;
};

// One node in the flattened schema, including groups and primitive leaves.
struct SchemaNode {
    // Root-to-leaf names excluding the schema root, e.g. {"address", "city"}.
    std::vector<std::string> path;
    // GROUP is our marker for a container node, not a Parquet primitive type.
    // physical_type describes bytes; logical_type describes their meaning (e.g. DATE).
    std::string name, physical_type, repetition, logical_type, converted_type;
    int32_t children = 0;
    std::optional<int32_t> type_length, scale, precision, field_id;
    bool signed_interpretation = false;
    bool date = false;
    bool flat_required = false;
    std::string column_order = "absent";
};

// The storage description of one column within one row group; no decoded values.
struct Chunk {
    std::vector<std::string> path;
    std::optional<std::string> external_file;
    bool metadata_present = false;
    bool encrypted = false;
    bool declares_v2 = false;
    std::string physical_type, codec;
    std::vector<std::string> encodings;
    int64_t file_offset = 0;
    std::optional<int64_t> values, data_offset, dictionary_offset;
    std::optional<int64_t> compressed_size, uncompressed_size;
    std::optional<Statistics> statistics;
};

// Metadata for a horizontal group of rows and all its column chunks.
struct RowGroup {
    int64_t rows = 0;
    int64_t uncompressed_size = 0;
    std::vector<Chunk> columns;
};

// Owned application metadata. No Thrift objects or references to footer buffers.
struct Metadata {
    bool encrypted = false;
    uint64_t file_size = 0, footer_offset = 0, footer_length = 0;
    int32_t version = 0;
    int64_t rows = 0;
    std::optional<std::string> created_by;
    std::vector<SchemaNode> schema;
    std::vector<RowGroup> row_groups;
    std::vector<std::string> limitations;
    // Application read accounting, not physical disk I/O.
    uint64_t read_bytes = 0, read_calls = 0;
    std::vector<ReadRange> reads;
};

} // namespace parquetscope
