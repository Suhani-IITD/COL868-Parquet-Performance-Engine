# Apache Parquet Format 2.10.0

Source tag: `apache-parquet-format-2.10.0` in
https://github.com/apache/parquet-format/tree/apache-parquet-format-2.10.0

`parquet.thrift`, `LICENSE` and `NOTICE` were retrieved on 2026-11-16 from
that tag's raw GitHub URLs using the browser fetch tool. The shell fetch script
failed DNS resolution, and shell network escalation was disabled. The browser's
line-numbered text was reconstructed without changing schema definitions,
comments, namespaces or attribution. **Whitespace is normalized by that tool;
these are not claimed to be byte-identical upstream downloads.** All 1101
extracted schema lines, 189 license lines and 5 notice lines were retained.
`SHA256SUMS` records the actual local files, not independently verified upstream
checksums. This is a documented acquisition deviation from the original handoff.

No network access is needed to build the vendored version.

CMake changes only the build copy's C++ namespace to `parquetscope_thrift` and
runs the installed Apache Thrift compiler. Generated files stay in the build
directory. The custom core's public headers contain no Thrift or Arrow types.
