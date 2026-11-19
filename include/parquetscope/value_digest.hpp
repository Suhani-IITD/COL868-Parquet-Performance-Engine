#pragma once
#include <cstdint>
#include <span>
#include <string>
#include <vector>

struct evp_md_ctx_st;
namespace parquetscope {

// SHA256 over row-major signed values, encoded as eight little-endian bytes each.
// This optional validation digest does not encode column schema or row count.
class ValueDigest {
public:
    // Initialize the SHA256 context, or throw if allocation/initialization fails.
    ValueDigest();
    // Release the owned OpenSSL context.
    ~ValueDigest();
    ValueDigest(const ValueDigest&) = delete;
    ValueDigest& operator=(const ValueDigest&) = delete;
    // Append one projected row to the digest; scratch storage is reused across rows.
    void row(std::span<const int64_t> values);
    // Finalize once and return a hexadecimal SHA256 string.
    std::string finish();

private:
    evp_md_ctx_st* context_ = nullptr;
    std::vector<uint8_t> bytes_;
    bool finished_ = false;
};
} // namespace parquetscope
