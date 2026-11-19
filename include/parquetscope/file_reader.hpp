#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <span>

namespace parquetscope {

// A successful read's location and length, not a copy of the bytes themselves.
struct ReadRange {
    uint64_t offset;
    uint64_t bytes;
};

// Owns one Linux file descriptor. Reads return exactly the requested bytes or throw.
class FileReader {
public:
    // Open a regular file and record its size and timestamps.
    explicit FileReader(const std::string& path);
    // Close the owned file descriptor.
    ~FileReader();
    FileReader(const FileReader&) = delete;
    FileReader& operator=(const FileReader&) = delete;

    // Return exactly the requested byte interval or throw.
    std::vector<uint8_t> read(uint64_t offset, uint64_t length);
    // Fill caller-owned storage exactly; counters accumulate across repeated reads.
    void read_into(uint64_t offset, std::span<uint8_t> destination);
    // Return a process-local identity binding metadata to this reader's lifetime.
    uint64_t identity() const { return identity_; }
    // Disable range history for repeated benchmarks; byte/call counters stay enabled.
    void record_ranges(bool enabled) { record_ranges_ = enabled; }
    // Throw if size or timestamps differ from those recorded at opening.
    void verify_unchanged() const;

    // Return the file size recorded at opening.
    uint64_t size() const { return file_size_; }
    // Return the total number of bytes actually received.
    uint64_t bytes_read() const { return total_bytes_read_; }
    // Return the number of attempted pread calls, including interruptions.
    uint64_t read_calls() const { return read_call_count_; }
    // Return successful read offsets/lengths; this view belongs to the reader.
    const std::vector<ReadRange>& ranges() const { return read_ranges_; }

private:
    uint64_t identity_ = 0;
    bool record_ranges_ = true;
    int file_descriptor_ = -1;
    uint64_t file_size_ = 0;
    uint64_t total_bytes_read_ = 0;
    uint64_t read_call_count_ = 0;
    int64_t modification_seconds_ = 0;
    int64_t modification_nanoseconds_ = 0;
    int64_t change_seconds_ = 0;
    int64_t change_nanoseconds_ = 0;
    std::vector<ReadRange> read_ranges_;
};

} // namespace parquetscope
