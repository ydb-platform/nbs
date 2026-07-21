IF (OS_LINUX)
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
        contrib/ydb/library/actors/dnsresolver
        contrib/ydb/library/actors/interconnect
        contrib/ydb/library/pdisk_io
        contrib/ydb/tools/stress_tool/lib
        contrib/ydb/tools/stress_tool/proto
        contrib/ydb/library/yql/minikql/comp_nodes/llvm16
        contrib/ydb/library/yql/parser/pg_wrapper
        contrib/ydb/library/yql/sql/pg
        contrib/ydb/library/yql/providers/yt/comp_nodes/dq/llvm16
        contrib/ydb/library/yql/providers/yt/comp_nodes/llvm16
    )

    SRCS(
        device_test_tool.cpp
    )

    END()

    RECURSE_FOR_TESTS(
        ut
    )
ENDIF(OS_LINUX)
