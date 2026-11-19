#include "parquetscope/value_digest.hpp"
#include <stdexcept>

namespace parquetscope {
// Encode signed values without overflow and update an order-sensitive fingerprint.
void ValueDigest::row(std::span<const int64_t> values) {
    if (finished_) {
        throw std::runtime_error("invalid digest update");
    }
    if (!initialized_) {
        summaries_.resize(values.size());
        initialized_ = true;
    }
    if (values.size() != summaries_.size()) {
        throw std::runtime_error("digest row width changed");
    }
    for (size_t column = 0; column < values.size(); ++column) {
        const auto value = values[column];
        auto& summary = summaries_[column];
        ++summary.count;
        summary.sum_modulo_64 += static_cast<uint64_t>(value);
        if (!summary.minimum || value < *summary.minimum) { summary.minimum = value; }
        if (!summary.maximum || value > *summary.maximum) { summary.maximum = value; }
        for (unsigned byte = 0; byte < 8; ++byte) {
            hash_ ^= static_cast<uint8_t>(static_cast<uint64_t>(value) >> (8 * byte));
            hash_ *= 1099511628211ull;
        }
    }
}

// Format the final FNV-1a state; repeated finalization is a caller error.
std::string ValueDigest::finish() {
    if (finished_) { throw std::runtime_error("digest already finalized"); }
    finished_ = true;
    constexpr char digits[] = "0123456789abcdef";
    std::string result(16, '0');
    for (unsigned digit = 0; digit < 16; ++digit) {
        result[15 - digit] = digits[(hash_ >> (digit * 4)) & 15];
    }
    return result;
}
} // namespace parquetscope
