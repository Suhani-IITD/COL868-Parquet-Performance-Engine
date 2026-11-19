#pragma once
#include <cstdint>
#include <span>
#include <string>
#include <vector>
#include <optional>

namespace parquetscope {
struct ValueSummary {
    uint64_t count = 0, sum_modulo_64 = 0;
    std::optional<int64_t> minimum, maximum;
};

// Optional correctness fingerprint: FNV-1a-64 over row-major little-endian INT64.
// Non-cryptographic; combine with counts/column summaries and independent row checks.
class ValueDigest {
public:
    // Update the hash and per-column summaries with one projected row.
    void row(std::span<const int64_t> values);
    // Finalize once and return 16 lowercase hexadecimal digits.
    std::string finish();
    // Borrow per-column counts, modulo-2^64 sums and signed extrema.
    const std::vector<ValueSummary>& summaries() const { return summaries_; }
private:
    uint64_t hash_ = 14695981039346656037ull;
    bool finished_ = false;
    bool initialized_ = false;
    std::vector<ValueSummary> summaries_;
};
} // namespace parquetscope
