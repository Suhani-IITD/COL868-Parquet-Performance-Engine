#pragma once

#include "parquetscope/metadata.hpp"

namespace parquetscope {

// Footer policy is separate from FileReader's per-read allocation limit.
inline constexpr uint64_t max_footer_bytes = 64 * 1024 * 1024;

// Read only header, trailer and footer; convert into owned metadata.
Metadata read_metadata(FileReader& file);
// Convenience entry point that opens and closes the input file.
Metadata inspect(const std::string& path);

} // namespace parquetscope
