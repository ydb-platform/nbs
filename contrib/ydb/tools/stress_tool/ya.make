PROGRAM(ydb_stress_tool)


PEERDIR(
    library/cpp/getopt
    contrib/ydb/apps/version
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/lwtrace_probes
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/load_test
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/tablet
    contrib/ydb/library/pdisk_io
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
    yql/essentials/minikql/comp_nodes/llvm16
    contrib/ydb/tools/stress_tool/lib
    contrib/ydb/tools/stress_tool/proto
)

SRCS(
    device_test_tool.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
