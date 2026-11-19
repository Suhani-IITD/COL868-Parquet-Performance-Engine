#pragma once

#include "parquetscope/metadata.hpp"
#include <span>
#include <array>
#include "parquetscope/timing.hpp"

namespace parquetscope {
inline constexpr uint64_t max_chunk_bytes = 64 * 1024 * 1024;
inline constexpr size_t max_page_header_bytes = 1024 * 1024;
inline constexpr size_t max_dictionary_bytes = 64 * 1024 * 1024;

// Aggregate diagnostics; no per-value history is stored.
struct DecodeCounts {
    uint64_t data_pages = 0, dictionary_pages = 0;
    uint64_t plain_values = 0, dictionary_values = 0;
    uint64_t rle_values = 0, bit_packed_values = 0, dictionary_entries = 0;
    // Dictionary data-page counts at each ID width (0..32), not footer features.
    std::array<uint64_t, 33> bit_width_pages{};
    uint64_t rle_runs = 0, bit_packed_runs = 0, checksums_verified = 0;
};

struct DecodePredicate {
    int64_t lower = std::numeric_limits<int64_t>::min();
    int64_t upper = std::numeric_limits<int64_t>::max();
    bool active = false;
    std::span<uint8_t> match_mask{};
};

struct DecodeOptions {
    bool naive_bit_unpacking = false;
    bool separate_dictionary_ids = false;
    ScanTiming* timing = nullptr;
    DecodePredicate predicate{};
};

// Decode exactly output.size() signed values from 4- or 8-byte PLAIN input.
void decode_plain(std::span<const uint8_t> bytes, size_t width, std::span<int64_t> output);
// Expand hybrid dictionary IDs into output; input begins with the ID bit width.
void decode_dictionary(std::span<const uint8_t> bytes, std::span<const int64_t> dictionary,
                       std::span<int64_t> output, DecodeCounts* counts = nullptr, DecodeOptions options = {},
                       std::span<const uint8_t> dict_matches = {}, std::span<uint8_t> match_mask = {});
// Decode and validate hybrid IDs without gathering values; output is caller-owned.
void decode_dictionary_indices(std::span<const uint8_t> bytes, size_t dictionary_size,
                               std::span<uint32_t> output, DecodeCounts* counts = nullptr,
                               DecodeOptions options = {},
                               std::span<const uint8_t> dict_matches = {}, std::span<uint8_t> match_mask = {});
// Gather validated dictionary IDs into signed output; reject mismatched/out-of-range input.
void gather_dictionary(std::span<const uint32_t> ids, std::span<const int64_t> dictionary,
                       std::span<int64_t> output);
// Decode a complete required, uncompressed column chunk into an already sized buffer.
void decode_chunk(std::span<const uint8_t> bytes, const Chunk& chunk,
                  std::span<int64_t> output, DecodeCounts& counts, DecodeOptions options = {});
} // namespace parquetscope
