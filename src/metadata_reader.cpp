#include "parquetscope/metadata_reader.hpp"
#include "parquet_types.h"

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <algorithm>
#include <bit>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace parquetscope {
namespace {

// Generated types stay private to this file; callers receive our Metadata structs.
namespace parquet_thrift = parquetscope_thrift;

struct FooterLocation {
    uint64_t offset;
    uint32_t length;
};

// Ordinary Parquet layout: PAR1 | pages | footer | uint32 length | PAR1.
// Read file markers/trailer and return the checked footer offset and length.
FooterLocation locate_footer(FileReader& file) {
    if (file.size() < 12) {
        throw std::runtime_error("file too short for Parquet header and trailer");
    }
    const auto header = file.read(0, 4);
    if (std::string(header.begin(), header.end()) != "PAR1") {
        throw std::runtime_error("invalid header magic (encrypted footers are unsupported)");
    }
    const auto trailer = file.read(file.size() - 8, 8);
    if (std::string(trailer.begin() + 4, trailer.end()) != "PAR1") {
        throw std::runtime_error("invalid trailer magic (encrypted footers are unsupported)");
    }

    // Explicit little-endian assembly works without alignment or host-endian assumptions.
    const uint32_t footer_length = uint32_t(trailer[0]) |
        (uint32_t(trailer[1]) << 8) | (uint32_t(trailer[2]) << 16) |
        (uint32_t(trailer[3]) << 24);
    if (footer_length == 0 || footer_length > file.size() - 12) {
        throw std::runtime_error("invalid footer length");
    }
    if (footer_length > max_footer_bytes) {
        throw std::runtime_error("footer exceeds 64 MiB resource limit");
    }
    return {file.size() - 8 - footer_length, footer_length};
}

// Decode borrowed footer bytes into owned Thrift metadata, enforcing parser limits.
parquet_thrift::FileMetaData deserialize_footer(std::vector<uint8_t>& footer_bytes) {
    // Thrift's APIs use shared pointers. OBSERVE borrows footer_bytes for this call;
    // the returned generated metadata owns its deserialized strings and containers.
    auto configuration = std::make_shared<apache::thrift::TConfiguration>(
        static_cast<int>(max_footer_bytes), static_cast<int>(max_footer_bytes), 64);
    auto memory_buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
        footer_bytes.data(), static_cast<uint32_t>(footer_bytes.size()),
        apache::thrift::transport::TMemoryBuffer::OBSERVE, configuration);
    apache::thrift::protocol::TCompactProtocol protocol(
        memory_buffer, static_cast<int32_t>(max_footer_bytes), 100000);

    parquet_thrift::FileMetaData thrift_metadata;
    try {
        thrift_metadata.read(&protocol);
    } catch (const std::exception& error) {
        throw std::runtime_error(std::string("Thrift footer: ") + error.what());
    }
    if (memory_buffer->available_read() != 0 &&
        !thrift_metadata.__isset.encryption_algorithm) {
        throw std::runtime_error("trailing bytes after Thrift metadata");
    }
    return thrift_metadata;
}

// Return a readable enum label, preserving unknown codes in the output.
template<class Enum>
std::string enum_name(Enum value, const std::map<int, const char*>& names) {
    const auto entry = names.find(static_cast<int>(value));
    if (entry == names.end()) {
        return "UNKNOWN(" + std::to_string(static_cast<int>(value)) + ")";
    }
    return entry->second;
}

// Append a diagnostic only if the metadata has not already recorded it.
void add_limitation(Metadata& metadata, const std::string& message) {
    if (std::find(metadata.limitations.begin(), metadata.limitations.end(), message) ==
        metadata.limitations.end()) {
        metadata.limitations.push_back(message);
    }
}

// Return the active logical annotation description; reject conflicting union members.
std::string describe_logical_type(const parquet_thrift::LogicalType& logical_type) {
    std::vector<std::string> active_types;
    // Preserve the generated description, including parameters such as decimal scale.
    auto append_type = [&](bool present, const char* name, const auto& value) {
        if (present) {
            std::ostringstream description;
            description << name << ':';
            value.printTo(description);
            active_types.push_back(description.str());
        }
    };
    append_type(logical_type.__isset.STRING, "STRING", logical_type.STRING);
    append_type(logical_type.__isset.MAP, "MAP", logical_type.MAP);
    append_type(logical_type.__isset.LIST, "LIST", logical_type.LIST);
    append_type(logical_type.__isset.ENUM, "ENUM", logical_type.ENUM);
    append_type(logical_type.__isset.DECIMAL, "DECIMAL", logical_type.DECIMAL);
    append_type(logical_type.__isset.DATE, "DATE", logical_type.DATE);
    append_type(logical_type.__isset.TIME, "TIME", logical_type.TIME);
    append_type(logical_type.__isset.TIMESTAMP, "TIMESTAMP", logical_type.TIMESTAMP);
    append_type(logical_type.__isset.INTEGER, "INTEGER", logical_type.INTEGER);
    append_type(logical_type.__isset.UNKNOWN, "UNKNOWN", logical_type.UNKNOWN);
    append_type(logical_type.__isset.JSON, "JSON", logical_type.JSON);
    append_type(logical_type.__isset.BSON, "BSON", logical_type.BSON);
    append_type(logical_type.__isset.UUID, "UUID", logical_type.UUID);
    append_type(logical_type.__isset.FLOAT16, "FLOAT16", logical_type.FLOAT16);
    if (active_types.size() > 1) {
        throw std::runtime_error("logical type union has multiple active members");
    }
    if (active_types.empty()) {
        return "unknown logical type";
    }
    return active_types.front();
}

// Preserve statistic bytes as hex and interpret signed bounds when the schema permits.
Bound convert_bound(const std::string& bytes, const SchemaNode& schema_node) {
    Bound result;
    constexpr char hex_digits[] = "0123456789abcdef";
    for (unsigned char byte : bytes) {
        result.raw_hex += hex_digits[byte >> 4];
        result.raw_hex += hex_digits[byte & 15];
    }
    if (schema_node.signed_interpretation) {
        size_t width = 8;
        if (schema_node.physical_type == "INT32") {
            width = 4;
        }
        if (bytes.size() != width) {
            throw std::runtime_error("invalid signed statistics byte width");
        }
        uint64_t bits = 0;
        for (size_t index = 0; index < width; ++index) {
            bits |= uint64_t(static_cast<unsigned char>(bytes[index])) << (8 * index);
        }
        if (width == 4) {
            result.signed_value = int64_t(std::bit_cast<int32_t>(static_cast<uint32_t>(bits)));
        } else {
            result.signed_value = std::bit_cast<int64_t>(bits);
        }
    }
    return result;
}

// Reject interpreted min/max pairs whose minimum exceeds their maximum.
void validate_bound_order(const std::optional<Bound>& minimum,
                          const std::optional<Bound>& maximum) {
    if (minimum && maximum && minimum->signed_value && maximum->signed_value &&
        *minimum->signed_value > *maximum->signed_value) {
        throw std::runtime_error("statistics minimum exceeds maximum");
    }
}

// Copy present statistics into owned fields, validating counts and bound ordering.
Statistics convert_statistics(const parquet_thrift::Statistics& statistics,
                              const SchemaNode& schema_node, int64_t value_count) {
    Statistics result;
    // __isset preserves the distinction between a missing field and a zero value.
    if (statistics.__isset.min_value) {
        result.min_value = convert_bound(statistics.min_value, schema_node);
    }
    if (statistics.__isset.max_value) {
        result.max_value = convert_bound(statistics.max_value, schema_node);
    }
    if (statistics.__isset.min) {
        result.legacy_min = convert_bound(statistics.min, schema_node);
    }
    if (statistics.__isset.max) {
        result.legacy_max = convert_bound(statistics.max, schema_node);
    }
    if (statistics.__isset.null_count) {
        if (statistics.null_count < 0 || statistics.null_count > value_count) {
            throw std::runtime_error("invalid null count");
        }
        result.null_count = statistics.null_count;
    }
    if (statistics.__isset.distinct_count) {
        if (statistics.distinct_count < 0) {
            throw std::runtime_error("negative distinct count");
        }
        result.distinct_count = statistics.distinct_count;
    }
    if (statistics.__isset.is_min_value_exact) {
        result.min_exact = statistics.is_min_value_exact;
    }
    if (statistics.__isset.is_max_value_exact) {
        result.max_exact = statistics.is_max_value_exact;
    }
    validate_bound_order(result.min_value, result.max_value);
    validate_bound_order(result.legacy_min, result.legacy_max);
    return result;
}

// Modern annotations take precedence, but conflicting legacy annotations must not
// cause unsigned/decimal statistics to be interpreted as signed integer bounds.
void set_numeric_interpretation(const parquet_thrift::SchemaElement& element,
                               SchemaNode& schema_node) {
    bool is_date = false;
    bool is_signed_integer = false;
    if (element.__isset.logicalType) {
        is_date = element.logicalType.__isset.DATE;
        is_signed_integer = element.logicalType.__isset.INTEGER &&
            element.logicalType.INTEGER.isSigned &&
            ((element.type == parquet_thrift::Type::INT32 &&
              element.logicalType.INTEGER.bitWidth == 32) ||
             (element.type == parquet_thrift::Type::INT64 &&
              element.logicalType.INTEGER.bitWidth == 64));
    } else {
        is_date = element.__isset.converted_type &&
            element.converted_type == parquet_thrift::ConvertedType::DATE;
        is_signed_integer = !element.__isset.converted_type ||
            (element.type == parquet_thrift::Type::INT32 &&
             element.converted_type == parquet_thrift::ConvertedType::INT_32) ||
            (element.type == parquet_thrift::Type::INT64 &&
             element.converted_type == parquet_thrift::ConvertedType::INT_64);
    }
    const bool legacy_compatible = !element.__isset.converted_type ||
        (is_date && element.converted_type == parquet_thrift::ConvertedType::DATE) ||
        (!is_date &&
         ((element.type == parquet_thrift::Type::INT32 &&
           element.converted_type == parquet_thrift::ConvertedType::INT_32) ||
          (element.type == parquet_thrift::Type::INT64 &&
           element.converted_type == parquet_thrift::ConvertedType::INT_64)));
    schema_node.date = is_date && element.__isset.type &&
        element.type == parquet_thrift::Type::INT32;
    schema_node.signed_interpretation = element.__isset.type && legacy_compatible &&
        ((is_date && schema_node.date) ||
         (!is_date && is_signed_integer &&
          (element.type == parquet_thrift::Type::INT32 ||
           element.type == parquet_thrift::Type::INT64)));
}

// Translate one schema element, its path and requiredness into an owned node.
SchemaNode convert_schema_node(const parquet_thrift::SchemaElement& element,
                               const std::vector<std::string>& path,
                               bool is_root, bool all_required, size_t depth) {
    SchemaNode result;
    result.path = path;
    result.name = element.name;
    if (is_root) {
        result.repetition = "ROOT";
    } else {
        result.repetition = enum_name(element.repetition_type, parquet_thrift::_FieldRepetitionType_VALUES_TO_NAMES);
    }
    if (element.__isset.type) {
        result.physical_type = enum_name(element.type, parquet_thrift::_Type_VALUES_TO_NAMES);
    } else {
        result.physical_type = "GROUP";
    }
    if (element.__isset.converted_type) {
        result.converted_type = enum_name(
            element.converted_type, parquet_thrift::_ConvertedType_VALUES_TO_NAMES);
    }
    if (element.__isset.logicalType) {
        result.logical_type = describe_logical_type(element.logicalType);
    }
    if (element.__isset.type_length) {
        result.type_length = element.type_length;
    }
    if (element.__isset.scale) {
        result.scale = element.scale;
    }
    if (element.__isset.precision) {
        result.precision = element.precision;
    }
    if (element.__isset.field_id) {
        result.field_id = element.field_id;
    }
    set_numeric_interpretation(element, result);
    result.flat_required = depth == 1 && all_required;
    return result;
}

// Thrift stores the schema in preorder. cursor advances past each complete subtree;
// leaf_indices maps column-chunk order to primitive entries in our schema vector.
void convert_schema_subtree(const std::vector<parquet_thrift::SchemaElement>& schema,
                            size_t& cursor, std::vector<std::string> path,
                            bool all_required, size_t depth, Metadata& metadata,
                            std::vector<size_t>& leaf_indices) {
    if (depth > 64) {
        throw std::runtime_error("schema exceeds depth limit 64");
    }
    if (cursor >= schema.size()) {
        throw std::runtime_error("schema child count exceeds available nodes");
    }
    const auto& element = schema[cursor++];
    const bool is_root = depth == 0;
    if (element.__isset.type == element.__isset.num_children) {
        throw std::runtime_error("schema node must be either a group or a primitive");
    }
    if (is_root && element.__isset.type) {
        throw std::runtime_error("schema root must be a group");
    }
    if (!is_root && !element.__isset.repetition_type) {
        throw std::runtime_error("non-root schema node missing repetition");
    }
    if (!is_root &&
        (element.repetition_type < parquet_thrift::FieldRepetitionType::REQUIRED ||
         element.repetition_type > parquet_thrift::FieldRepetitionType::REPEATED)) {
        throw std::runtime_error("invalid schema repetition");
    }
    if (!is_root) {
        path.push_back(element.name);
    }
    all_required = all_required &&
        (is_root || element.repetition_type == parquet_thrift::FieldRepetitionType::REQUIRED);
    const auto schema_node = convert_schema_node(element, path, is_root, all_required, depth);
    const size_t node_index = metadata.schema.size();
    metadata.schema.push_back(schema_node);

    if (element.__isset.type) {
        if (element.type == parquet_thrift::Type::FIXED_LEN_BYTE_ARRAY &&
            (!element.__isset.type_length || element.type_length <= 0)) {
            throw std::runtime_error("invalid fixed-length byte array width");
        }
        leaf_indices.push_back(node_index);
        if (!schema_node.flat_required) {
            add_limitation(metadata, "nested, optional or repeated columns are outside the initial scan subset");
        }
        if (!schema_node.signed_interpretation) {
            add_limitation(metadata, "physical/logical type outside signed INT32, INT64 and DATE scan subset; bounds retained as raw hex");
        }
    } else {
        if (element.num_children < 0) {
            throw std::runtime_error("negative schema child count");
        }
        metadata.schema[node_index].children = element.num_children;
        std::set<std::string> sibling_names;
        for (int32_t child = 0; child < element.num_children; ++child) {
            if (cursor >= schema.size()) {
                throw std::runtime_error("missing schema child");
            }
            if (!sibling_names.insert(schema[cursor].name).second) {
                throw std::runtime_error("duplicate sibling schema name");
            }
            convert_schema_subtree(schema, cursor, path, all_required, depth + 1,
                                   metadata, leaf_indices);
        }
    }
}

// Convert the full schema and return leaf indices in column-chunk order.
std::vector<size_t> convert_schema(const parquet_thrift::FileMetaData& thrift_metadata,
                                   Metadata& metadata) {
    if (thrift_metadata.schema.empty()) {
        throw std::runtime_error("schema is empty");
    }
    std::vector<size_t> leaf_indices;
    size_t cursor = 0;
    convert_schema_subtree(thrift_metadata.schema, cursor, {}, true, 0,
                           metadata, leaf_indices);
    if (cursor != thrift_metadata.schema.size()) {
        throw std::runtime_error("unreachable schema nodes");
    }
    if (thrift_metadata.__isset.column_orders) {
        if (thrift_metadata.column_orders.size() != leaf_indices.size()) {
            throw std::runtime_error("column order count does not match schema leaves");
        }
        for (size_t column = 0; column < leaf_indices.size(); ++column) {
            if (thrift_metadata.column_orders[column].__isset.TYPE_ORDER) {
                metadata.schema[leaf_indices[column]].column_order = "TYPE_ORDER";
            } else {
                metadata.schema[leaf_indices[column]].column_order = "unknown";
            }
        }
    }
    return leaf_indices;
}

// Check populated page offsets and chunk size against the file data region.
void validate_chunk_offsets(const Chunk& chunk, uint64_t footer_offset) {
    if (*chunk.data_offset < 0 || (chunk.dictionary_offset && *chunk.dictionary_offset < 0)) {
        throw std::runtime_error("negative page offset");
    }
    // External column files are described, but their ranges cannot be checked here.
    if (!chunk.external_file && *chunk.compressed_size > 0) {
        int64_t start = *chunk.data_offset;
        if (chunk.dictionary_offset) {
            start = std::min(*chunk.dictionary_offset, *chunk.data_offset);
        }
        if (start < 4 || uint64_t(start) > footer_offset ||
            uint64_t(*chunk.compressed_size) > footer_offset - uint64_t(start)) {
            throw std::runtime_error("column chunk range overlaps header/footer or exceeds file");
        }
        if (uint64_t(*chunk.data_offset - start) >= uint64_t(*chunk.compressed_size)) {
            throw std::runtime_error("data page offset outside chunk");
        }
        if (chunk.dictionary_offset && *chunk.dictionary_offset > *chunk.data_offset) {
            throw std::runtime_error("dictionary offset follows data page offset");
        }
    }
}

// Copy codec/encoding descriptions and record unsupported scan features.
void convert_encodings(const parquet_thrift::ColumnMetaData& column_metadata,
                       Chunk& chunk, Metadata& metadata) {
    chunk.codec = enum_name(column_metadata.codec, parquet_thrift::_CompressionCodec_VALUES_TO_NAMES);
    if (column_metadata.codec != parquet_thrift::CompressionCodec::UNCOMPRESSED) {
        add_limitation(metadata, "compressed column chunks are outside the initial scan subset");
    }
    for (auto encoding : column_metadata.encodings) {
        chunk.encodings.push_back(enum_name(encoding, parquet_thrift::_Encoding_VALUES_TO_NAMES));
        if (encoding != parquet_thrift::Encoding::PLAIN &&
            encoding != parquet_thrift::Encoding::PLAIN_DICTIONARY &&
            encoding != parquet_thrift::Encoding::RLE &&
            encoding != parquet_thrift::Encoding::BIT_PACKED &&
            encoding != parquet_thrift::Encoding::RLE_DICTIONARY) {
            add_limitation(metadata, "declared encoding outside plain/dictionary/RLE/bit-packed scan subset");
        }
    }
    if (column_metadata.__isset.encoding_stats) {
        for (const auto& encoding : column_metadata.encoding_stats) {
            if (encoding.page_type == parquet_thrift::PageType::DATA_PAGE_V2) {
                chunk.declares_v2 = true;
                add_limitation(metadata, "V2 data pages declared; outside initial V1 scan subset");
            }
        }
    }
}

// Validate and translate one column chunk against its schema and row-group count.
Chunk convert_column_chunk(const parquet_thrift::ColumnChunk& thrift_chunk,
                           const SchemaNode& schema_node, int64_t row_count,
                           Metadata& metadata) {
    Chunk chunk;
    chunk.path = schema_node.path;
    chunk.file_offset = thrift_chunk.file_offset;
    if (thrift_chunk.file_offset < 0) {
        throw std::runtime_error("negative column file offset");
    }
    if (thrift_chunk.__isset.file_path && !thrift_chunk.file_path.empty()) {
        chunk.external_file = thrift_chunk.file_path;
        add_limitation(metadata, "external column files are described but not opened");
    }
    chunk.encrypted = thrift_chunk.__isset.crypto_metadata ||
        thrift_chunk.__isset.encrypted_column_metadata;
    if (chunk.encrypted) {
        add_limitation(metadata, "encrypted columns are unsupported for scanning");
    }
    chunk.metadata_present = thrift_chunk.__isset.meta_data;
    if (!chunk.metadata_present) {
        add_limitation(metadata, "column metadata absent; external/encrypted metadata is not resolved");
        return chunk;
    }

    const auto& column_metadata = thrift_chunk.meta_data;
    if (column_metadata.path_in_schema != schema_node.path) {
        throw std::runtime_error("column path does not match schema");
    }
    chunk.physical_type = enum_name(column_metadata.type, parquet_thrift::_Type_VALUES_TO_NAMES);
    if (chunk.physical_type != schema_node.physical_type) {
        throw std::runtime_error("column type does not match schema");
    }
    if (column_metadata.num_values < 0 || column_metadata.total_compressed_size < 0 ||
        column_metadata.total_uncompressed_size < 0) {
        throw std::runtime_error("negative chunk count or size");
    }
    if (schema_node.flat_required && column_metadata.num_values != row_count) {
        throw std::runtime_error("required column value count differs from row count");
    }
    chunk.values = column_metadata.num_values;
    chunk.data_offset = column_metadata.data_page_offset;
    chunk.compressed_size = column_metadata.total_compressed_size;
    chunk.uncompressed_size = column_metadata.total_uncompressed_size;
    if (column_metadata.__isset.dictionary_page_offset) {
        chunk.dictionary_offset = column_metadata.dictionary_page_offset;
    }
    // The required fields above are populated before the offset helper uses them.
    validate_chunk_offsets(chunk, metadata.footer_offset);
    convert_encodings(column_metadata, chunk, metadata);
    if (column_metadata.__isset.statistics) {
        chunk.statistics = convert_statistics(
            column_metadata.statistics, schema_node, column_metadata.num_values);
    }
    return chunk;
}

// Append validated row groups/chunks and verify their total row count.
void convert_row_groups(const parquet_thrift::FileMetaData& thrift_metadata,
                        const std::vector<size_t>& leaf_indices, Metadata& metadata) {
    int64_t row_sum = 0;
    for (const auto& thrift_group : thrift_metadata.row_groups) {
        if (thrift_group.num_rows < 0 || thrift_group.total_byte_size < 0) {
            throw std::runtime_error("negative row group count or size");
        }
        if (thrift_group.num_rows > std::numeric_limits<int64_t>::max() - row_sum) {
            throw std::runtime_error("row count overflow");
        }
        row_sum += thrift_group.num_rows;
        if (thrift_group.columns.size() != leaf_indices.size()) {
            throw std::runtime_error("row group column count does not match schema leaves");
        }
        RowGroup group;
        group.rows = thrift_group.num_rows;
        group.uncompressed_size = thrift_group.total_byte_size;
        for (size_t column = 0; column < thrift_group.columns.size(); ++column) {
            const auto& schema_node = metadata.schema[leaf_indices[column]];
            group.columns.push_back(convert_column_chunk(
                thrift_group.columns[column], schema_node, thrift_group.num_rows, metadata));
        }
        metadata.row_groups.push_back(std::move(group));
    }
    if (row_sum != thrift_metadata.num_rows) {
        throw std::runtime_error("file row count differs from row group sum");
    }
}

// Conversion is separate from I/O and deserialization, but remains in this file.
Metadata convert_metadata(const parquet_thrift::FileMetaData& thrift_metadata,
                          uint64_t file_size, FooterLocation footer) {
    Metadata metadata;
    metadata.file_size = file_size;
    metadata.footer_offset = footer.offset;
    metadata.footer_length = footer.length;
    metadata.version = thrift_metadata.version;
    metadata.rows = thrift_metadata.num_rows;
    if (thrift_metadata.version <= 0 || thrift_metadata.num_rows < 0) {
        throw std::runtime_error("invalid metadata version or row count");
    }
    if (thrift_metadata.__isset.created_by) {
        metadata.created_by = thrift_metadata.created_by;
    }
    metadata.encrypted = thrift_metadata.__isset.encryption_algorithm;
    if (metadata.encrypted) {
        add_limitation(metadata, "encrypted file: authentication and encrypted columns are unsupported");
    }
    const auto leaf_indices = convert_schema(thrift_metadata, metadata);
    convert_row_groups(thrift_metadata, leaf_indices, metadata);
    add_limitation(metadata, "page-level compatibility unverified: no pages read by inspection; scan validates selected chunks");
    return metadata;
}

} // namespace

// Read header/trailer/footer, convert metadata and return its read accounting.
Metadata read_metadata(FileReader& file) {
    const auto footer = locate_footer(file);
    auto footer_bytes = file.read(footer.offset, footer.length);
    const auto thrift_metadata = deserialize_footer(footer_bytes);
    auto metadata = convert_metadata(thrift_metadata, file.size(), footer);

    metadata.reader_identity = file.identity();
    metadata.read_bytes = file.bytes_read();
    metadata.read_calls = file.read_calls();
    metadata.reads = file.ranges();
    return metadata;
}

// Open a file and return owned metadata; the descriptor closes before return.
Metadata inspect(const std::string& path) {
    FileReader file(path);
    return read_metadata(file);
}

} // namespace parquetscope
