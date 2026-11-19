cmake_minimum_required(VERSION 3.20)
project(parquet_performance_engine VERSION 0.1 LANGUAGES CXX)

# Default single-configuration builds to optimized code; explicit Debug remains valid.
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build configuration" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
find_package(nlohmann_json REQUIRED)
pkg_check_modules(THRIFT REQUIRED IMPORTED_TARGET thrift)
find_program(THRIFT_COMPILER thrift REQUIRED)

# Generate private wire types from the pinned schema. Only the build copy changes.
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/third_party/parquet_format/parquet.thrift" FORMAT_SCHEMA)
string(REPLACE "namespace cpp parquet" "namespace cpp parquetscope_thrift"
       FORMAT_SCHEMA "${FORMAT_SCHEMA}")
file(MAKE_DIRECTORY "${GENERATED_DIR}")
file(CONFIGURE OUTPUT "${GENERATED_DIR}/parquet.thrift" CONTENT "${FORMAT_SCHEMA}" @ONLY)
add_custom_command(
    OUTPUT "${GENERATED_DIR}/parquet_types.cpp" "${GENERATED_DIR}/parquet_types.h"
    COMMAND "${THRIFT_COMPILER}" --gen cpp:no_skeleton -out "${GENERATED_DIR}"
            "${GENERATED_DIR}/parquet.thrift"
    DEPENDS "${GENERATED_DIR}/parquet.thrift"
    VERBATIM
)
add_library(parquet_thrift STATIC "${GENERATED_DIR}/parquet_types.cpp")
target_include_directories(parquet_thrift PUBLIC "${GENERATED_DIR}")
target_link_libraries(parquet_thrift PUBLIC PkgConfig::THRIFT)

add_library(parquet_metadata_core
    src/file_reader.cpp src/metadata_reader.cpp
    src/query_parser.cpp src/planner.cpp src/plan_json.cpp
    src/decoder.cpp src/scanner.cpp src/scan_json.cpp src/value_digest.cpp
)
target_include_directories(parquet_metadata_core PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(parquet_metadata_core PRIVATE parquet_thrift
    PUBLIC nlohmann_json::nlohmann_json)
target_compile_options(parquet_metadata_core PRIVATE -Wall -Wextra -Wpedantic -Wconversion)

add_executable(parquet_metadata src/main.cpp)
target_link_libraries(parquet_metadata PRIVATE parquet_metadata_core)
target_compile_options(parquet_metadata PRIVATE -Wall -Wextra -Wpedantic -Wconversion)
