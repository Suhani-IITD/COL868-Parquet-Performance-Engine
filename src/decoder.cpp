#include "parquetscope/decoder.hpp"
#include "parquet_types.h"

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <cstring>
#include <type_traits>
#include <stdexcept>

namespace parquetscope {
namespace {
namespace parquet_thrift = parquetscope_thrift;

// Read a hybrid run header, advancing position past its variable-length integer.
uint32_t read_run_header(std::span<const uint8_t> bytes, size_t& position) {
    uint32_t value = 0;
    for (unsigned index = 0; index < 5; ++index) {
        if (position >= bytes.size()) {
            throw std::runtime_error("decode: truncated hybrid run header");
        }
        const auto byte = bytes[position++];
        if (index == 4 && (byte & 0xf0) != 0) {
            throw std::runtime_error("decode: overflowing hybrid run header");
        }
        value |= uint32_t(byte & 0x7f) << (7 * index);
        if ((byte & 0x80) == 0) {
            return value;
        }
    }
    throw std::runtime_error("decode: unterminated hybrid run header");
}

// Build the IEEE CRC32 lookup table at compile time; return all 256 entries.
constexpr auto make_crc_table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t index = 0; index < 256; ++index) {
        auto value = index;
        for (unsigned bit = 0; bit < 8; ++bit) {
            if ((value & 1) != 0) {
                value = (value >> 1) ^ 0xedb88320u;
            } else {
                value >>= 1;
            }
        }
        table[index] = value;
    }
    return table;
}

// Compute the checksum of one encoded page body; input is borrowed, not copied.
uint32_t calculate_crc32(std::span<const uint8_t> bytes) {
    static constexpr auto table = make_crc_table();
    uint32_t checksum = 0xffffffffu;
    for (auto byte : bytes) {
        checksum = table[(checksum ^ byte) & 0xff] ^ (checksum >> 8);
    }
    return checksum ^ 0xffffffffu;
}

// Only page fields needed for decoding; generated Thrift types remain private.
struct PageHeader {
    bool dictionary = false;
    size_t header_bytes = 0, body_bytes = 0;
    int32_t values = 0, encoding = 0;
    std::optional<uint32_t> checksum;
};

// Reject unsupported or inconsistent page variants and sizes before using them.
void validate_page_header(const parquet_thrift::PageHeader& header) {
    if (header.type == parquet_thrift::PageType::DATA_PAGE_V2) {
        throw std::runtime_error("decode: V2 data pages unsupported");
    }
    if (header.type != parquet_thrift::PageType::DATA_PAGE &&
        header.type != parquet_thrift::PageType::DICTIONARY_PAGE) {
        throw std::runtime_error("decode: unsupported page type " + std::to_string(header.type));
    }
    const int variants = int(header.__isset.data_page_header) +
        int(header.__isset.dictionary_page_header) + int(header.__isset.index_page_header) +
        int(header.__isset.data_page_header_v2);
    if (variants != 1) {
        throw std::runtime_error("decode: page must have exactly one matching subheader");
    }
    if (header.compressed_page_size < 0 || header.uncompressed_page_size < 0) {
        throw std::runtime_error("decode: negative page size");
    }
    if (header.compressed_page_size != header.uncompressed_page_size) {
        throw std::runtime_error("decode: uncompressed page size mismatch");
    }
}

// Check a V1 header's level-encoding declaration; required flat columns have no level payload.
bool supported_level_encoding(parquet_thrift::Encoding::type encoding) {
    return encoding == parquet_thrift::Encoding::RLE || encoding == parquet_thrift::Encoding::BIT_PACKED;
}

// Translate a validated Thrift header and check that its body fits the chunk remainder.
PageHeader convert_page_header(const parquet_thrift::PageHeader& header,
                               size_t header_bytes, size_t available_bytes) {
    PageHeader page;
    page.header_bytes = header_bytes;
    page.body_bytes = static_cast<size_t>(header.compressed_page_size);
    if (page.body_bytes > available_bytes - header_bytes) {
        throw std::runtime_error("decode: page body exceeds chunk boundary");
    }
    if (header.__isset.crc) {
        page.checksum = static_cast<uint32_t>(header.crc);
    }
    page.dictionary = header.type == parquet_thrift::PageType::DICTIONARY_PAGE;
    if (page.dictionary) {
        if (!header.__isset.dictionary_page_header) {
            throw std::runtime_error("decode: dictionary page missing dictionary header");
        }
        page.values = header.dictionary_page_header.num_values;
        page.encoding = header.dictionary_page_header.encoding;
    } else {
        if (!header.__isset.data_page_header) {
            throw std::runtime_error("decode: data page missing data header");
        }
        const auto& data = header.data_page_header;
        page.values = data.num_values;
        page.encoding = data.encoding;
        if (!supported_level_encoding(data.definition_level_encoding) ||
            !supported_level_encoding(data.repetition_level_encoding)) {
            throw std::runtime_error("decode: unsupported level encoding declaration");
        }
    }
    if (page.values < 0) {
        throw std::runtime_error("decode: negative page value count");
    }
    return page;
}

// Deserialize the header at the start of bytes and return its owned description.
PageHeader read_page_header(std::span<const uint8_t> bytes) {
    using apache::thrift::transport::TMemoryBuffer;
    const auto window = static_cast<uint32_t>(std::min(bytes.size(), max_page_header_bytes));
    if (window == 0) {
        throw std::runtime_error("decode: missing page header");
    }
    static thread_local auto configuration = std::make_shared<apache::thrift::TConfiguration>(
        int(max_page_header_bytes), int(max_page_header_bytes), 64);
    // OBSERVE does not modify these bytes, despite Thrift's mutable-pointer interface.
    auto buffer = std::make_shared<TMemoryBuffer>(const_cast<uint8_t*>(bytes.data()),
        window, TMemoryBuffer::OBSERVE, configuration);
    apache::thrift::protocol::TCompactProtocol protocol(buffer, int(max_page_header_bytes), 100000);
    parquet_thrift::PageHeader header;
    try {
        header.read(&protocol);
    } catch (const std::exception& error) {
        throw std::runtime_error(std::string("decode: invalid or oversized page header: ") + error.what());
    }
    validate_page_header(header);
    return convert_page_header(header, window - buffer->available_read(), bytes.size());
}

// Load up to eight little-endian bytes without alignment assumptions or tail overreads.
uint64_t load_little_endian(std::span<const uint8_t> bytes) {
    uint64_t word = 0;
    const auto length = std::min(bytes.size(), sizeof(word));
    if constexpr (std::endian::native == std::endian::little) {
        if (length == sizeof(word)) {
            std::memcpy(&word, bytes.data(), sizeof(word));
        } else if (length != 0) {
            std::memcpy(&word, bytes.data(), length);
        }
    } else {
        for (size_t byte = 0; byte < length; ++byte) {
            word |= uint64_t(bytes[byte]) << (8 * byte);
        }
    }
    return word;
}

// Expand one repeated ID into output, advance the input position and return values written.
template<bool Indices>
size_t decode_rle_run(std::span<const uint8_t> bytes, size_t& position,
                      uint8_t bit_width, uint64_t repetitions,
                      std::span<const int64_t> dictionary, size_t dictionary_size,
                      std::span<std::conditional_t<Indices, uint32_t, int64_t>> output) {
    if (repetitions > output.size()) {
        throw std::runtime_error("decode: RLE run exceeds page value count");
    }
    const size_t value_bytes = (size_t(bit_width) + 7) / 8;
    if (value_bytes > bytes.size() - position) {
        throw std::runtime_error("decode: truncated RLE repeated value");
    }
    uint32_t dictionary_id = 0;
    for (size_t byte = 0; byte < value_bytes; ++byte) {
        dictionary_id |= uint32_t(bytes[position++]) << (8 * byte);
    }
    if (uint64_t(dictionary_id) >= (uint64_t{1} << bit_width)) {
        throw std::runtime_error("decode: RLE value exceeds declared bit width");
    }
    if (dictionary_id >= dictionary_size) {
        throw std::runtime_error("decode: dictionary ID out of range");
    }
    if constexpr (Indices) {
        std::fill_n(output.begin(), static_cast<size_t>(repetitions), dictionary_id);
    } else {
        std::fill_n(output.begin(), static_cast<size_t>(repetitions), dictionary[dictionary_id]);
    }
    return static_cast<size_t>(repetitions);
}

// Expand groups of eight packed IDs; allow at most seven padding values at page end.
template<bool Indices, bool Naive>
size_t decode_bit_packed_run(std::span<const uint8_t> bytes, size_t& position,
                             uint8_t bit_width, uint64_t groups,
                             std::span<const int64_t> dictionary, size_t dictionary_size,
                             std::span<std::conditional_t<Indices, uint32_t, int64_t>> output) {
    const uint64_t run_values = groups * 8;
    if (run_values > uint64_t(std::numeric_limits<int32_t>::max())) {
        throw std::runtime_error("decode: bit-packed run exceeds format length limit");
    }
    if (run_values > output.size() && run_values - output.size() > 7) {
        throw std::runtime_error("decode: excess bit-packed padding");
    }
    const uint64_t packed_bytes = groups * bit_width;
    if (packed_bytes > bytes.size() - position) {
        throw std::runtime_error("decode: truncated bit-packed run");
    }
    const auto values_to_decode = static_cast<size_t>(std::min<uint64_t>(run_values, output.size()));
    for (size_t index = 0; index < values_to_decode; ++index) {
        uint32_t dictionary_id = 0;
        if constexpr (Naive) {
            for (unsigned bit = 0; bit < bit_width; ++bit) {
                const size_t bit_position = index * bit_width + bit;
                dictionary_id |= uint32_t((bytes[position + bit_position / 8] >> (bit_position % 8)) & 1u) << bit;
            }
        } else if (bit_width != 0) {
            const size_t bit_position = index * bit_width;
            const size_t byte_position = bit_position / 8;
            const auto word = load_little_endian(bytes.subspan(position + byte_position,
                static_cast<size_t>(packed_bytes) - byte_position));
            const uint64_t mask = (uint64_t{1} << bit_width) - 1;
            dictionary_id = static_cast<uint32_t>((word >> (bit_position % 8)) & mask);
        }
        if (dictionary_id >= dictionary_size) {
            throw std::runtime_error("decode: dictionary ID out of range");
        }
        if constexpr (Indices) {
            output[index] = dictionary_id;
        } else {
            output[index] = dictionary[dictionary_id];
        }
    }
    position += static_cast<size_t>(packed_bytes);
    return values_to_decode;
}

// Validate a complete raw chunk against footer metadata; return its starting file offset.
int64_t validate_chunk_input(std::span<const uint8_t> bytes, const Chunk& chunk,
                             size_t output_values) {
    if (chunk.codec != "UNCOMPRESSED") {
        throw std::runtime_error("decode: compressed chunk unsupported");
    }
    if (chunk.physical_type != "INT32" && chunk.physical_type != "INT64") {
        throw std::runtime_error("decode: unsupported physical type");
    }
    if (!chunk.compressed_size || *chunk.compressed_size < 0 || uint64_t(*chunk.compressed_size) != bytes.size()) {
        throw std::runtime_error("decode: chunk length mismatch");
    }
    if (!chunk.uncompressed_size || *chunk.uncompressed_size < 0 || uint64_t(*chunk.uncompressed_size) != bytes.size()) {
        throw std::runtime_error("decode: uncompressed chunk length mismatch");
    }
    if (!chunk.values || *chunk.values < 0 || uint64_t(*chunk.values) != output_values) {
        throw std::runtime_error("decode: chunk value count mismatch");
    }
    if (bytes.size() > max_chunk_bytes) {
        throw std::runtime_error("decode: chunk exceeds 64 MiB resource limit");
    }
    if (!chunk.data_offset || *chunk.data_offset < 4) {
        throw std::runtime_error("decode: missing/invalid data offset");
    }
    const auto start = chunk.dictionary_offset.value_or(*chunk.data_offset);
    if (start < 4 || start > *chunk.data_offset) {
        throw std::runtime_error("decode: invalid dictionary offset");
    }
    return start;
}

// Mutable state is local to one chunk; its dictionary is reused only across that chunk's pages.
struct ChunkDecodeState {
    std::vector<int64_t> dictionary;
    std::vector<uint32_t> indices;
    size_t written_values = 0;
    bool saw_dictionary = false;
    bool saw_data = false;
};

// Load the first/only dictionary page into state after validating its position and size.
void decode_dictionary_page(const PageHeader& page, std::span<const uint8_t> body,
                            size_t page_offset, size_t value_width, const Chunk& chunk,
                            ChunkDecodeState& state, DecodeCounts& counts) {
    if (state.saw_dictionary || state.saw_data || page_offset != 0) {
        throw std::runtime_error("decode: dictionary must occur once at start of chunk");
    }
    if (!chunk.dictionary_offset) {
        throw std::runtime_error("decode: dictionary page missing footer offset");
    }
    if (page.encoding != parquet_thrift::Encoding::PLAIN &&
        page.encoding != parquet_thrift::Encoding::PLAIN_DICTIONARY) {
        throw std::runtime_error("decode: dictionary page must use PLAIN encoding");
    }
    const auto values = static_cast<size_t>(page.values);
    if (values > max_dictionary_bytes / sizeof(int64_t)) {
        throw std::runtime_error("decode: dictionary exceeds decoded resource limit");
    }
    if (body.size() % value_width != 0 || body.size() / value_width != values) {
        throw std::runtime_error("decode: dictionary payload length mismatch");
    }
    state.dictionary.resize(values);
    decode_plain(body, value_width, state.dictionary);
    state.saw_dictionary = true;
    ++counts.dictionary_pages;
    counts.dictionary_entries += values;
}

// Decode one data page into the remaining output and update chunk progress/counters.
void decode_data_page(const PageHeader& page, std::span<const uint8_t> body,
                      size_t value_width, std::span<int64_t> output,
                      ChunkDecodeState& state, DecodeCounts& counts, DecodeOptions options) {
    const auto values = static_cast<size_t>(page.values);
    if (values > output.size() - state.written_values) {
        throw std::runtime_error("decode: page values exceed column count");
    }
    auto destination = output.subspan(state.written_values, values);
    if (page.encoding == parquet_thrift::Encoding::PLAIN) {
        decode_plain(body, value_width, destination);
        counts.plain_values += values;
    } else if (page.encoding == parquet_thrift::Encoding::PLAIN_DICTIONARY ||
               page.encoding == parquet_thrift::Encoding::RLE_DICTIONARY) {
        if (!state.saw_dictionary) {
            throw std::runtime_error("decode: dictionary data page precedes dictionary");
        }
        if (options.separate_dictionary_ids) {
            // The extra ID buffer is intentional only for this experimental mode.
            auto started = start_timer(options.timing != nullptr);
            state.indices.resize(values);
            if (options.timing) { add_elapsed(true, started, options.timing->index_allocation_ms); }
            started = start_timer(options.timing != nullptr);
            decode_dictionary_indices(body, state.dictionary.size(), state.indices, &counts, options);
            if (options.timing) { add_elapsed(true, started, options.timing->index_decode_ms); }
            started = start_timer(options.timing != nullptr);
            gather_dictionary(state.indices, state.dictionary, destination);
            if (options.timing) { add_elapsed(true, started, options.timing->gather_ms); }
        } else {
            decode_dictionary(body, state.dictionary, destination, &counts, options);
        }
        counts.dictionary_values += values;
    } else {
        throw std::runtime_error("decode: unsupported data encoding " + std::to_string(page.encoding));
    }
    state.written_values += values;
    state.saw_data = true;
    ++counts.data_pages;
}

// Check final counts and dictionary presence after every page has been consumed.
void validate_chunk_completion(const Chunk& chunk, const ChunkDecodeState& state, size_t output_values) {
    if (state.written_values != output_values) {
        throw std::runtime_error("decode: column ended before declared value count");
    }
    if (state.saw_dictionary != chunk.dictionary_offset.has_value()) {
        throw std::runtime_error("decode: footer dictionary offset has no dictionary page");
    }
    if (!state.saw_data && output_values != 0) {
        throw std::runtime_error("decode: column has no data pages");
    }
}
} // namespace

// Interpret fixed-width little-endian bytes as signed values, writing every output element.
void decode_plain(std::span<const uint8_t> bytes, size_t width, std::span<int64_t> output) {
    if (width != 4 && width != 8) {
        throw std::runtime_error("decode: PLAIN width must be 4 or 8");
    }
    if (bytes.size() % width != 0 || bytes.size() / width != output.size()) {
        throw std::runtime_error("decode: PLAIN payload length differs from value count");
    }
    if constexpr (std::endian::native == std::endian::little) {
        if (width == 8) {
            if (!output.empty()) { std::memcpy(output.data(), bytes.data(), bytes.size()); }
            return;
        }
        for (size_t index = 0; index < output.size(); ++index) {
            int32_t value;
            std::memcpy(&value, bytes.data() + index * 4, sizeof(value));
            output[index] = value;
        }
    } else {
        for (size_t index = 0; index < output.size(); ++index) {
            const auto bits = load_little_endian(bytes.subspan(index * width, width));
            if (width == 4) {
                output[index] = std::bit_cast<int32_t>(static_cast<uint32_t>(bits));
            } else {
                output[index] = std::bit_cast<int64_t>(bits);
            }
        }
    }
}

// Decode the bit-width prefix and successive RLE/packed runs into signed dictionary values.
template<bool Indices>
void decode_hybrid(std::span<const uint8_t> bytes, std::span<const int64_t> dictionary,
                   size_t dictionary_size, std::span<std::conditional_t<Indices, uint32_t, int64_t>> output,
                   DecodeCounts* counts, DecodeOptions options) {
    if (bytes.empty()) {
        throw std::runtime_error("decode: missing dictionary ID bit width");
    }
    const auto bit_width = bytes[0];
    if (bit_width > 32) {
        throw std::runtime_error("decode: dictionary ID bit width exceeds 32");
    }
    if (counts) { ++counts->bit_width_pages[bit_width]; }
    size_t position = 1;
    size_t written_values = 0;
    while (written_values < output.size()) {
        const auto run_header = read_run_header(bytes, position);
        const uint64_t units = run_header >> 1;
        if (units == 0) {
            throw std::runtime_error("decode: zero-length hybrid run");
        }
        auto remaining_output = output.subspan(written_values);
        if ((run_header & 1) == 0) {
            const auto written = decode_rle_run<Indices>(bytes, position, bit_width, units, dictionary, dictionary_size, remaining_output);
            written_values += written;
            if (counts) {
                ++counts->rle_runs;
                counts->rle_values += written;
            }
        } else {
            size_t written;
            if (options.naive_bit_unpacking) {
                written = decode_bit_packed_run<Indices, true>(bytes, position, bit_width, units, dictionary, dictionary_size, remaining_output);
            } else {
                written = decode_bit_packed_run<Indices, false>(bytes, position, bit_width, units, dictionary, dictionary_size, remaining_output);
            }
            written_values += written;
            if (counts) {
                ++counts->bit_packed_runs;
                counts->bit_packed_values += written;
            }
        }
    }
    if (position != bytes.size()) {
        throw std::runtime_error("decode: trailing dictionary ID bytes");
    }
}

// Expand hybrid IDs directly into values without allocating an intermediate ID buffer.
void decode_dictionary(std::span<const uint8_t> bytes, std::span<const int64_t> dictionary,
                       std::span<int64_t> output, DecodeCounts* counts, DecodeOptions options) {
    decode_hybrid<false>(bytes, dictionary, dictionary.size(), output, counts, options);
}

// Return validated IDs for experiments that measure unpacking and gathering separately.
void decode_dictionary_indices(std::span<const uint8_t> bytes, size_t dictionary_size,
                               std::span<uint32_t> output, DecodeCounts* counts, DecodeOptions options) {
    decode_hybrid<true>(bytes, {}, dictionary_size, output, counts, options);
}

// Materialize IDs into values, preserving range checks for this standalone entry point.
void gather_dictionary(std::span<const uint32_t> ids, std::span<const int64_t> dictionary,
                       std::span<int64_t> output) {
    if (ids.size() != output.size()) { throw std::runtime_error("decode: gather size mismatch"); }
    for (size_t index = 0; index < ids.size(); ++index) {
        if (ids[index] >= dictionary.size()) { throw std::runtime_error("decode: dictionary ID out of range"); }
        output[index] = dictionary[ids[index]];
    }
}

// Walk a complete chunk's pages, verify checksums and fill the supplied column buffer.
void decode_chunk(std::span<const uint8_t> bytes, const Chunk& chunk,
                  std::span<int64_t> output, DecodeCounts& counts, DecodeOptions options) {
    const auto chunk_start = validate_chunk_input(bytes, chunk, output.size());
    size_t value_width = 8;
    if (chunk.physical_type == "INT32") {
        value_width = 4;
    }
    ChunkDecodeState state;
    size_t position = 0;
    while (position < bytes.size()) {
        // Locate this page's header/body without copying the underlying chunk bytes.
        const size_t page_offset = position;
        auto stage_start = start_timer(options.timing != nullptr);
        const auto page = read_page_header(bytes.subspan(position));
        if (options.timing) { add_elapsed(true, stage_start, options.timing->page_header_ms); }
        position += page.header_bytes;
        const auto body = bytes.subspan(position, page.body_bytes);
        position += page.body_bytes;

        if (page.checksum) {
            stage_start = start_timer(options.timing != nullptr);
            if (calculate_crc32(body) != *page.checksum) {
                throw std::runtime_error("decode: page CRC32 mismatch");
            }
            ++counts.checksums_verified;
            if (options.timing) { add_elapsed(true, stage_start, options.timing->checksum_ms); }
        }
        if (page.dictionary) {
            decode_dictionary_page(page, body, page_offset, value_width, chunk, state, counts);
        } else {
            // The first data page must agree with the footer's absolute offset.
            if (!state.saw_data && uint64_t(chunk_start) + page_offset != uint64_t(*chunk.data_offset)) {
                throw std::runtime_error("decode: first data page differs from footer offset");
            }
            decode_data_page(page, body, value_width, output, state, counts, options);
        }
    }
    validate_chunk_completion(chunk, state, output.size());
}
} // namespace parquetscope
