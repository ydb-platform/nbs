LIBRARY()

SRCS(
    actors.cpp
    kqp_runner.cpp
    ydb_setup.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/workload_service/actors
    contrib/ydb/core/testlib

    contrib/ydb/tests/tools/kqprun/runlib
    contrib/ydb/tests/tools/kqprun/src/proto
)

GENERATE_ENUM_SERIALIZATION(common.h)

YQL_LAST_ABI_VERSION()

END()
