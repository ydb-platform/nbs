PY3TEST()

SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

TEST_SRCS(
    tpc_tests.py
)

DEPENDS(
    contrib/ydb/library/benchmarks/runner/run_tests
    contrib/ydb/library/yql/tools/dqrun
    contrib/ydb/library/benchmarks/gen_queries
    contrib/ydb/library/benchmarks/runner/result_compare
    contrib/ydb/library/benchmarks/runner/runner

    yql/essentials/udfs/common/set
    yql/essentials/udfs/common/url_base
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/math
    yql/essentials/udfs/common/unicode_base
)

DATA_FILES(
    contrib/ydb/library/yql/tools/dqrun/examples/fs.conf
    contrib/ydb/library/benchmarks/runner/runner/test-gateways.conf
    contrib/tools/flame-graph

    contrib/ydb/library/benchmarks/runner/download_lib.sh
    contrib/ydb/library/benchmarks/runner/download_tables.sh
    contrib/ydb/library/benchmarks/runner/download_tpcds_tables.sh
    contrib/ydb/library/benchmarks/runner/download_files_ds_1.sh
    contrib/ydb/library/benchmarks/runner/download_files_ds_10.sh
    contrib/ydb/library/benchmarks/runner/download_files_ds_100.sh
    contrib/ydb/library/benchmarks/runner/download_files_h_1.sh
    contrib/ydb/library/benchmarks/runner/download_files_h_10.sh
    contrib/ydb/library/benchmarks/runner/download_files_h_100.sh

    contrib/ydb/library/benchmarks/runner/upload_results.py
)

END()

RECURSE(
    run_tests
    runner
    result_convert
    result_compare
)
