#include "parquetscope/file_reader.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>

namespace parquetscope {

// Open a read-only regular file and save its size/timestamps for later checks.
FileReader::FileReader(const std::string& path) {
    // Read-only access; do not carry this descriptor into an executed program.
    file_descriptor_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (file_descriptor_ < 0) {
        throw std::runtime_error("open " + path + ": " + std::strerror(errno));
    }

    struct stat file_status {};
    if (::fstat(file_descriptor_, &file_status) != 0 ||
        !S_ISREG(file_status.st_mode) || file_status.st_size < 0) {
        // A failed constructor does not run this object's destructor.
        ::close(file_descriptor_);
        file_descriptor_ = -1;
        throw std::runtime_error("input must be a readable regular file");
    }

    file_size_ = static_cast<uint64_t>(file_status.st_size);
    modification_seconds_ = file_status.st_mtim.tv_sec;
    modification_nanoseconds_ = file_status.st_mtim.tv_nsec;
    change_seconds_ = file_status.st_ctim.tv_sec;
    change_nanoseconds_ = file_status.st_ctim.tv_nsec;
}

// Close the owned descriptor when the reader leaves scope.
FileReader::~FileReader() {
    if (file_descriptor_ >= 0) {
        ::close(file_descriptor_);
    }
}

// Compare current size/timestamps with the opening snapshot and throw on change.
void FileReader::verify_unchanged() const {
    struct stat file_status {};
    if (::fstat(file_descriptor_, &file_status) != 0) {
        throw std::runtime_error("cannot recheck input file");
    }

    // Detect changes since opening.
    if (file_status.st_size < 0 ||
        static_cast<uint64_t>(file_status.st_size) != file_size_ ||
        file_status.st_mtim.tv_sec != modification_seconds_ ||
        file_status.st_mtim.tv_nsec != modification_nanoseconds_ ||
        file_status.st_ctim.tv_sec != change_seconds_ ||
        file_status.st_ctim.tv_nsec != change_nanoseconds_) {
        throw std::runtime_error("input file changed since it was opened");
    }
}

// Return exactly length bytes at offset, updating actual read counters or throwing.
std::vector<uint8_t> FileReader::read(uint64_t offset, uint64_t length) {
    // Subtraction avoids overflowing offset + length. Check offset first.
    if (offset > file_size_ || length > file_size_ - offset) {
        throw std::runtime_error("read range outside file");
    }
    // This limits one allocation/read request, not the total file size.
    constexpr uint64_t max_read_bytes = 64 * 1024 * 1024;
    if (length > max_read_bytes) {
        throw std::runtime_error("read exceeds 64 MiB resource limit");
    }

    std::vector<uint8_t> data(static_cast<size_t>(length));
    uint64_t completed_bytes = 0;
    while (completed_bytes < length) {
        // Count every syscall attempt, including interrupted/failed attempts.
        ++read_call_count_;
        const auto returned_bytes = ::pread(
            file_descriptor_, data.data() + completed_bytes,
            static_cast<size_t>(length - completed_bytes),
            static_cast<off_t>(offset + completed_bytes));

        if (returned_bytes < 0 && errno == EINTR) {
            continue; // No bytes returned: retry after an interrupted syscall.
        }
        if (returned_bytes < 0) {
            throw std::runtime_error(std::string("pread: ") + std::strerror(errno));
        }
        if (returned_bytes == 0) {
            throw std::runtime_error("unexpected end of file during read");
        }

        const auto received_bytes = static_cast<uint64_t>(returned_bytes);
        read_ranges_.push_back({offset + completed_bytes, received_bytes});
        total_bytes_read_ += received_bytes;
        completed_bytes += received_bytes;
        // A short successful read continues at the first byte still missing.
    }
    return data;
}

} // namespace parquetscope
