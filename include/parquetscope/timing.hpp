#pragma once
#include <chrono>

namespace parquetscope {
using Clock = std::chrono::steady_clock;

// Return milliseconds elapsed since a steady-clock timestamp.
inline double elapsed_ms(Clock::time_point start) {
    return std::chrono::duration<double, std::milli>(Clock::now() - start).count();
}

// Start an optional measurement; disabled timing performs no clock read.
inline Clock::time_point start_timer(bool enabled) {
    if (enabled) {
        return Clock::now();
    }
    return {};
}

// Add a stage's duration to its accumulator only when timing is enabled.
inline void add_elapsed(bool enabled, Clock::time_point start, double& total) {
    if (enabled) {
        total += elapsed_ms(start);
    }
}

struct ScanTiming {
    double planning_ms = 0, read_ms = 0, allocation_ms = 0;
    // Nested decode stages; these are included in decode_ms, not additional time.
    double page_header_ms = 0, checksum_ms = 0, index_decode_ms = 0, gather_ms = 0;
    double index_allocation_ms = 0;
    double decode_ms = 0, filter_ms = 0, execution_ms = 0;
};
} // namespace parquetscope
