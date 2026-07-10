const MACRO_KIND = {
  ADDINCL: "directory",
  ARCHIVE: "source-file",
  DATA: "data",
  DEPENDS: "module-reference-root-relative",
  DYNAMIC_LIBRARY_FROM: "module-reference-root-relative",
  EXPORTS_SCRIPT: "source-file",
  FILES: "source-file",
  GENERATE_ENUM_SERIALIZATION: "source-file",
  GO_GRPC_GATEWAY_SRCS: "source-file",
  GO_TEST_SRCS: "source-file",
  GO_TEST_FOR: "module-reference-root-relative",
  GO_XTEST_SRCS: "source-file",
  INCLUDE: "file",
  JOIN_SRCS: "source-file",
  PEERDIR: "module-directory-root-relative",
  PY_SRCS: "source-file",
  RECURSE: "module-directory-current-relative",
  RECURSE_FOR_TESTS: "module-directory-current-relative",
  RECURSE_ROOT_RELATIVE: "module-directory-root-relative",
  RESOURCE: "resource-file",
  RUN_PYTHON3: "source-file",
  SRCS: "source-file",
  SRCDIR: "directory",
  SUPPRESSIONS: "source-file",
  TEST_SRCS: "source-file",
  UNITTEST_FOR: "module-reference-root-relative",
  USE_RECIPE: "module-reference-root-relative",
};
const PATH_MACROS = new Set(Object.keys(MACRO_KIND));
const SOURCE_FILE_MACROS = new Set([
  "EXPORTS_SCRIPT",
  "FILES",
  "GENERATE_ENUM_SERIALIZATION",
  "GO_GRPC_GATEWAY_SRCS",
  "GO_TEST_SRCS",
  "GO_XTEST_SRCS",
  "JOIN_SRCS",
  "PY_SRCS",
  "RUN_PYTHON3",
  "SRCS",
  "SUPPRESSIONS",
  "TEST_SRCS",
]);
const SOURCE_ARG_KEYWORDS = new Set([
  "CYTHON_C",
  "CYTHON_C_API_H",
  "CYTHON_C_H",
  "CYTHON_CPP",
  "CYTHON_CPP_H",
  "CYTHONIZE_PY",
  "GLOBAL",
  "MAIN",
  "NAMESPACE",
  "NOAUTO",
  "NO_LINT",
  "OBJECT_DEPENDS",
  "PREFIX",
  "RENAME",
  "TOP_LEVEL",
]);
const SOURCE_ARG_KEYWORDS_WITH_VALUE = new Set(["NAMESPACE", "OBJECT_DEPENDS", "PREFIX", "RENAME"]);
const DIRECTORY_ARG_KEYWORDS = new Set(["GLOBAL", "LOCAL", "ONE_LEVEL"]);
const DIRECTORY_ARG_KEYWORDS_WITH_VALUE = new Set(["FOR"]);
const UNSUPPORTED_DATA_PREFIXES = ["sbr://", "ext:", "http://", "https://"];
const RESOURCE_MACROS = new Set(["FROM_SANDBOX"]);
const RESOURCE_MAPPING_FILES = ["build/ext_mapping.conf.json", "build/mapping.conf.json"];
const MODULE_DECLARATION_MACROS = new Set([
  "DLL",
  "GTEST",
  "G_BENCHMARK",
  "GO_LIBRARY",
  "GO_PROGRAM",
  "GO_TEST",
  "GO_TEST_FOR",
  "LIBRARY",
  "PACKAGE",
  "PROGRAM",
  "PROTO_LIBRARY",
  "PY23_LIBRARY",
  "PY2_LIBRARY",
  "PY2_PROGRAM",
  "PY2TEST",
  "PY3_LIBRARY",
  "PY3_PROGRAM",
  "PY3TEST",
  "UNITTEST",
]);
const MACRO_DOCS = {
  ADDINCL: {
    usage: "ADDINCL([FOR <lang>] [GLOBAL dir]* dirlist)",
    text: "Adds directories to include/import search paths for the current project. For C/C++ this becomes `-I<path>`; `FOR <lang>` targets another language and `GLOBAL` propagates the include path to dependent projects.",
  },
  ALLOCATOR: {
    usage: "ALLOCATOR(alloc)",
    text: "Selects the allocator variant for a program or library, for example `TCMALLOC` or `TCMALLOC_TC`.",
  },
  ARCHIVE: {
    usage: "ARCHIVE(NAME archive_name [DONT_COMPRESS] files...)",
    text: "Builds an archive resource from input files. `NAME` names the generated archive output; following file arguments are archive inputs.",
  },
  BENCHMARK_OPTS: {
    usage: "BENCHMARK_OPTS(options...)",
    text: "Adds command-line options for benchmark execution.",
  },
  BUILD_ONLY_IF: {
    usage: "BUILD_ONLY_IF([FATAL_ERROR | STRICT | WARNING] conditions...)",
    text: "Restricts module buildability to matching platform/configuration conditions.",
  },
  CFLAGS: {
    usage: "CFLAGS([GLOBAL compiler_flag]* compiler_flags)",
    text: "Adds C compiler flags to the current module. `GLOBAL` propagates the following flags to dependent modules.",
  },
  CHECK_DEPENDENT_DIRS: {
    usage: "CHECK_DEPENDENT_DIRS(DENY | ALLOW_ONLY ([ALL | PEERDIRS | GLOB] dir)...)",
    text: "Declares dependency directory restrictions for validation of dependent modules.",
  },
  CXXFLAGS: {
    usage: "CXXFLAGS(compiler_flags)",
    text: "Adds C++ compiler flags to the current module.",
  },
  DATA: {
    usage: "DATA(arcadia/path | sbr://resource | ...)",
    text: "Adds files or directories to test data. Local `arcadia/...` paths are copied into the test environment.",
  },
  DEFAULT: {
    usage: "DEFAULT(varname value)",
    text: "Sets a default variable value if it has not already been set.",
  },
  DEPENDS: {
    usage: "DEPENDS(path1 [path2...])",
    text: "Adds build dependencies that are needed by tests or runtime workflows but are not linked as normal library peers.",
  },
  DLL: {
    usage: "DLL(name major_ver [minor_ver] [EXPORTS symlist_file] [PREFIX prefix])",
    text: "Declares a shared library module.",
  },
  DYNAMIC_LIBRARY: {
    usage: "DYNAMIC_LIBRARY([name])",
    text: "Declares a dynamic-library module variant.",
  },
  DYNAMIC_LIBRARY_FROM: {
    usage: "DYNAMIC_LIBRARY_FROM(path/to/static/module)",
    text: "Builds a dynamic library from another module, typically a matching static library module.",
  },
  ELSE: {
    usage: "ELSE()",
    text: "Starts the false branch of a ymake conditional block.",
  },
  ELSEIF: {
    usage: "ELSEIF(condition)",
    text: "Starts an additional conditional branch in a ymake conditional block.",
  },
  END: {
    usage: "END()",
    text: "Ends the current module declaration.",
  },
  ENDIF: {
    usage: "ENDIF()",
    text: "Ends a ymake conditional block.",
  },
  ENV: {
    usage: "ENV(name=value ...)",
    text: "Adds environment variables for a test module.",
  },
  EXCLUDE_TAGS: {
    usage: "EXCLUDE_TAGS(tags...)",
    text: "Excludes tagged generated variants or files from the current module.",
  },
  EXPORTS_SCRIPT: {
    usage: "EXPORTS_SCRIPT(file)",
    text: "Declares a symbol exports script for a shared-library module.",
  },
  FILES: {
    usage: "FILES(files...)",
    text: "Declares extra files belonging to the current module.",
  },
  FORK_SUBTESTS: {
    usage: "FORK_SUBTESTS([mode...])",
    text: "Runs test subcases in separate processes. Often used together with `SPLIT_FACTOR` for parallel test execution.",
  },
  FROM_SANDBOX: {
    usage: "FROM_SANDBOX([FILE] resource_id [RENAME <resource files>] OUT_[NOAUTO] <output files> ...)",
    text: "Downloads a Sandbox/resource-mapping resource, optionally unpacks it, and declares output files for the build graph.",
  },
  FUZZ: {
    usage: "FUZZ()",
    text: "Declares a fuzz-test program module.",
  },
  GENERATE_ENUM_SERIALIZATION: {
    usage: "GENERATE_ENUM_SERIALIZATION(file.h)",
    text: "Generates and compiles enum string-conversion support for enum members declared in the header.",
  },
  GTEST: {
    usage: "GTEST([name])",
    text: "Defines a C++ unit test module based on `library/cpp/testing/gtest`. The module automatically peers `library/cpp/testing/gtest` and `library/cpp/testing/gtest_main`; the name is usually omitted.",
  },
  G_BENCHMARK: {
    usage: "G_BENCHMARK([benchmarkname])",
    text: "Defines a Google Benchmark-based benchmark program module.",
  },
  GO_TEST_SRCS: {
    usage: "GO_TEST_SRCS(files...)",
    text: "Declares Go sources for internal tests of the current Go module.",
  },
  GO_GRPC_GATEWAY_SRCS: {
    usage: "GO_GRPC_GATEWAY_SRCS(files...)",
    text: "Enables grpc-gateway generation for the listed Go/protobuf inputs.",
  },
  GO_LIBRARY: {
    usage: "GO_LIBRARY([name])",
    text: "Declares a Go library module.",
  },
  GO_PROGRAM: {
    usage: "GO_PROGRAM([name])",
    text: "Declares a Go executable module.",
  },
  GO_TEST: {
    usage: "GO_TEST([name])",
    text: "Declares a Go test module.",
  },
  GO_TEST_FOR: {
    usage: "GO_TEST_FOR(path/to/module)",
    text: "Declares a Go test module for another Go module and uses that module as the source root.",
  },
  GO_XTEST_SRCS: {
    usage: "GO_XTEST_SRCS(files...)",
    text: "Declares Go sources for external tests of the current Go module.",
  },
  GRPC: {
    usage: "GRPC()",
    text: "Enables gRPC code generation for protobuf sources in a `PROTO_LIBRARY`.",
  },
  IF: {
    usage: "IF(condition)",
    text: "Starts a ymake conditional block.",
  },
  INCLUDE: {
    usage: "INCLUDE(filename)",
    text: "Reads another makelist fragment. Relative paths are resolved from the current file; `${ARCADIA_ROOT}` is resolved from the workspace root.",
  },
  INCLUDE_TAGS: {
    usage: "INCLUDE_TAGS(tags...)",
    text: "Includes only matching tagged generated variants or files in the current module.",
  },
  JOIN_SRCS: {
    usage: "JOIN_SRCS(output.cpp input1.cpp [input2.cpp...])",
    text: "Generates one joined source file from C++ inputs by emitting `#include` directives for the input files, then sends the generated output through normal source processing.",
  },
  LIBRARY: {
    usage: "LIBRARY([name])",
    text: "Declares a C++ library module.",
  },
  LICENSE: {
    usage: "LICENSE(licenses...)",
    text: "Declares license metadata for the module.",
  },
  LICENSE_RESTRICTION_EXCEPTIONS: {
    usage: "LICENSE_RESTRICTION_EXCEPTIONS(paths...)",
    text: "Declares exceptions for license restriction checks.",
  },
  LICENSE_TEXTS: {
    usage: "LICENSE_TEXTS(file)",
    text: "Declares a file containing collected license texts for the module.",
  },
  MESSAGE: {
    usage: "MESSAGE(text...)",
    text: "Emits a configure-time message from the makelist.",
  },
  NO_COMPILER_WARNINGS: {
    usage: "NO_COMPILER_WARNINGS()",
    text: "Suppresses compiler warning diagnostics for the module.",
  },
  NO_RUNTIME: {
    usage: "NO_RUNTIME()",
    text: "Marks that the module should not link the default runtime support.",
  },
  NO_SANITIZE: {
    usage: "NO_SANITIZE()",
    text: "Disables sanitizer instrumentation for the module.",
  },
  NO_SPLIT_DWARF: {
    usage: "NO_SPLIT_DWARF()",
    text: "Disables split DWARF debug information for the module.",
  },
  NO_UTIL: {
    usage: "NO_UTIL()",
    text: "Disables the default util dependency for the module.",
  },
  ONLY_TAGS: {
    usage: "ONLY_TAGS(tags...)",
    text: "Keeps only matching tagged generated variants or files in the current module.",
  },
  PACKAGE: {
    usage: "PACKAGE(name)",
    text: "Declares a package aggregation module.",
  },
  PEERDIR: {
    usage: "PEERDIR(dirs...)",
    text: "Declares module dependencies. Library peers are linked into executable/shared targets that depend on this module.",
  },
  PROGRAM: {
    usage: "PROGRAM([progname])",
    text: "Declares a C++ executable module.",
  },
  PROTO_LIBRARY: {
    usage: "PROTO_LIBRARY()",
    text: "Declares a protobuf library module. It can produce generated code for enabled languages such as C++, Go, Python, or gRPC.",
  },
  PY23_LIBRARY: {
    usage: "PY23_LIBRARY([name])",
    text: "Declares a Python library module compatible with both Python 2 and Python 3 flows.",
  },
  PY2_PROGRAM: {
    usage: "PY2_PROGRAM([progname])",
    text: "Declares a Python 2 executable module.",
  },
  PY3_LIBRARY: {
    usage: "PY3_LIBRARY([name])",
    text: "Declares a Python 3 library module.",
  },
  PY3_PROGRAM: {
    usage: "PY3_PROGRAM([progname])",
    text: "Declares a Python 3 executable module.",
  },
  PY_MAIN: {
    usage: "PY_MAIN(package.module[:func])",
    text: "Sets the Python entry point for a Python program module.",
  },
  PY3TEST: {
    usage: "PY3TEST([name])",
    text: "Defines a Python 3 pytest-based test module. It is compatible with Python 3-tagged modules.",
  },
  PY_SRCS: {
    usage: "PY_SRCS({| CYTHON_C} { | TOP_LEVEL | NAMESPACE ns} files...)",
    text: "`PY_SRCS` embeds Python sources and generated Python outputs into Python build modules. `TOP_LEVEL` puts following files at the import root. `NAMESPACE ns` changes the module import prefix for following files to `ns.` instead of the default path-derived namespace.",
  },
  RESOURCE: {
    usage: "RESOURCE([FORCE_TEXT] [src key]* [- key=value]*)",
    text: "Embeds files or literal values as program resources. In `src key` pairs, `src` is the file and `key` is the runtime lookup name used by resource libraries.",
  },
  RECURSE: {
    usage: "RECURSE(dirs...)",
    text: "Adds child directories to the build traversal. Arguments are relative to the current `ya.make` directory.",
  },
  RECURSE_FOR_TESTS: {
    usage: "RECURSE_FOR_TESTS(dirs...)",
    text: "Adds directories to traversal when tests are requested. Use test traversal flags to include these in dependency dumps/build traversal.",
  },
  RECURSE_ROOT_RELATIVE: {
    usage: "RECURSE_ROOT_RELATIVE(dirlist)",
    text: "Adds directories to build traversal using paths relative to `${ARCADIA_ROOT}`.",
  },
  REQUIREMENTS: {
    usage: "REQUIREMENTS([cpu:<count>] [disk_usage:<size>] [ram:<size>] [container:<id>] ...)",
    text: "Declares test runtime requirements such as CPU, disk, RAM, container, network, or DNS needs.",
  },
  RUN_PYTHON3: {
    usage: "RUN_PYTHON3(script.py [args...])",
    text: "Runs a Python 3 script as part of the build graph.",
  },
  SET: {
    usage: "SET(varname value)",
    text: "Sets a ymake variable in the current configuration scope.",
  },
  SET_APPEND: {
    usage: "SET_APPEND(varname values...)",
    text: "Appends values to a ymake variable in the current configuration scope.",
  },
  SIZE: {
    usage: "SIZE(SMALL | MEDIUM | LARGE)",
    text: "Sets the test size label. Test size controls resource/timeout expectations in the test system; `SMALL` is the default and most restricted, while `LARGE` is the broadest.",
  },
  SKIP_TEST: {
    usage: "SKIP_TEST(reason...)",
    text: "Marks a test module as skipped.",
  },
  SPLIT_DWARF: {
    usage: "SPLIT_DWARF()",
    text: "Enables split DWARF debug information for the module.",
  },
  SRCS: {
    usage: "SRCS(files...)",
    text: "Declares source files. Paths are resolved from the current makelist, source roots such as `UNITTEST_FOR`, and then Arcadia root.",
  },
  SRCDIR: {
    usage: "SRCDIR(path)",
    text: "Adds a source directory used to resolve source file paths in the current module.",
  },
  SPLIT_FACTOR: {
    usage: "SPLIT_FACTOR(x)",
    text: "Sets the number of chunks for parallel test execution. With test modules, it works with `FORK_TESTS()` / `FORK_SUBTESTS()` and may imply test forking.",
  },
  SUPPRESSIONS: {
    usage: "SUPPRESSIONS(files...)",
    text: "Declares suppression files for test/runtime tooling.",
  },
  TAG: {
    usage: "TAG(tags...)",
    text: "Adds tags to a test module.",
  },
  TEST_SRCS: {
    usage: "TEST_SRCS(files...)",
    text: "Declares source files containing tests for the current test module.",
  },
  TIMEOUT: {
    usage: "TIMEOUT(seconds)",
    text: "Sets the timeout for the current test module.",
  },
  UNION: {
    usage: "UNION(name)",
    text: "Declares a union aggregation module.",
  },
  UNITTEST: {
    usage: "UNITTEST([name])",
    text: "Declares a C++ unit test module based on `library/cpp/testing/unittest`.",
  },
  UNITTEST_FOR: {
    usage: "UNITTEST_FOR(path/to/lib)",
    text: "Convenience C++ unit test module for another library. It adds `SRCDIR`, `ADDINCL`, and `PEERDIR` for the target library path.",
  },
  USE_RECIPE: {
    usage: "USE_RECIPE(path/to/recipe-module [args...])",
    text: "Attaches a test recipe module to the current test. The first argument is a root-relative recipe module path.",
  },
  USE_COMMON_GOOGLE_APIS: {
    usage: "USE_COMMON_GOOGLE_APIS([apis...])",
    text: "Enables common Google API protobuf imports, optionally limited to specific API groups.",
  },
  VERSION: {
    usage: "VERSION(args...)",
    text: "Declares version metadata for the module.",
  },
  WITHOUT_LICENSE_TEXTS: {
    usage: "WITHOUT_LICENSE_TEXTS()",
    text: "Disables license text collection for the module.",
  },
  Y_BENCHMARK: {
    usage: "Y_BENCHMARK([benchmarkname])",
    text: "Declares a Yandex benchmark program module.",
  },
  YQL_LAST_ABI_VERSION: {
    usage: "YQL_LAST_ABI_VERSION()",
    text: "Requests the latest YQL ABI version for the module.",
  },
};

module.exports = {
  MACRO_KIND,
  PATH_MACROS,
  SOURCE_FILE_MACROS,
  SOURCE_ARG_KEYWORDS,
  SOURCE_ARG_KEYWORDS_WITH_VALUE,
  DIRECTORY_ARG_KEYWORDS,
  DIRECTORY_ARG_KEYWORDS_WITH_VALUE,
  UNSUPPORTED_DATA_PREFIXES,
  RESOURCE_MACROS,
  RESOURCE_MAPPING_FILES,
  MODULE_DECLARATION_MACROS,
  MACRO_DOCS,
};
