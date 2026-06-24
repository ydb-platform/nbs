LIBRARY(ydb_device_test)


PEERDIR(
    contrib/libs/protobuf
    library/cpp/monlib/dynamic_counters/percentile
    contrib/ydb/core/blobstorage/lwtrace_probes
    contrib/ydb/core/load_test
    contrib/ydb/core/protos
    contrib/ydb/tools/stress_tool/proto
    contrib/ydb/library/actors/core
)

SRCS(
    ../device_test_tool.h
    ../device_test_tool_aio_test.h
    ../device_test_tool_driveestimator.h
    ../device_test_tool_trim_test.cpp
    ../device_test_tool_trim_test.h
    ../device_test_tool_pdisk_test.h
)

END()

RECURSE_FOR_TESTS(
    ../ut
)
