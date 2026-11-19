#pragma once

#include "parquetscope/metadata.hpp"
#include <span>

namespace parquetscope {
inline constexpr uint64_t max_chunk_bytes = 64 * 1024 * 1024;
inline constexpr size_t max_page_header_bytes = 1024 * 1024;
inline constexpr size_t max_dictionary_bytes = 64 * 1024 * 1024;

// Aggregate diagnostics; no per-value history is stored.
struct DecodeCounts {
    uint64_t data_pages = 0, dictionary_pages = 0;
    uint64_t plain_values = 0, dictionary_values = 0;
    uint64_t rle_runs = 0, bit_packed_runs = 0, checksums_verified = 0;
};

// Decode exactly output.size() signed values from 4- or 8-byte PLAIN input.
void decode_plain(std::span<const uint8_t> bytes, size_t width, std::span<int64_t> output);
// Expand hybrid dictionary IDs into output; input begins with the ID bit width.
void decode_dictionary(std::span<const uint8_t> bytes, std::span<const int64_t> dictionary,
                       std::span<int64_t> output, DecodeCounts* counts = nullptr);
// Decode a complete required, uncompressed column chunk into an already sized buffer.
void decode_chunk(std::span<const uint8_t> bytes, const Chunk& chunk,
                  std::span<int64_t> output, DecodeCounts& counts);
} // namespace parquetscope
