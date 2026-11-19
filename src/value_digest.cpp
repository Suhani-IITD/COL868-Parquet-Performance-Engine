#include "parquetscope/value_digest.hpp"
#include <openssl/evp.h>
#include <limits>
#include <stdexcept>

namespace parquetscope {

// Allocate and initialize the owned SHA256 context.
ValueDigest::ValueDigest() {
    context_ = EVP_MD_CTX_new();
    if (!context_ || EVP_DigestInit_ex(context_, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(context_);
        throw std::runtime_error("SHA256 initialization failed");
    }
}

// Release the context whether or not finish() was called.
ValueDigest::~ValueDigest() {
    EVP_MD_CTX_free(context_);
}

// Encode one row deterministically and append its bytes to the running digest.
void ValueDigest::row(std::span<const int64_t> values) {
    if (finished_ || values.size() > std::numeric_limits<size_t>::max() / 8) {
        throw std::runtime_error("invalid digest update");
    }
    bytes_.resize(values.size() * 8);
    for (size_t index = 0; index < values.size(); ++index) {
        for (size_t byte = 0; byte < 8; ++byte) {
            bytes_[index * 8 + byte] = static_cast<uint8_t>(uint64_t(values[index]) >> (8 * byte));
        }
    }
    if (EVP_DigestUpdate(context_, bytes_.data(), bytes_.size()) != 1) {
        throw std::runtime_error("SHA256 update failed");
    }
}

// Finish the hash and return its hexadecimal text; reject repeated finalization.
std::string ValueDigest::finish() {
    if (finished_) {
        throw std::runtime_error("digest already finalized");
    }
    unsigned char bytes[EVP_MAX_MD_SIZE];
    unsigned length = 0;
    if (EVP_DigestFinal_ex(context_, bytes, &length) != 1 || length != 32) {
        throw std::runtime_error("SHA256 finish failed");
    }
    finished_ = true;
    constexpr char hex_digits[] = "0123456789abcdef";
    std::string result;
    for (unsigned index = 0; index < length; ++index) {
        result += hex_digits[bytes[index] >> 4];
        result += hex_digits[bytes[index] & 15];
    }
    return result;
}
} // namespace parquetscope
