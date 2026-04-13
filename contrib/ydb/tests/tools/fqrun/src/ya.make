LIBRARY()

SRCS(
    common.cpp
    fq_runner.cpp
    fq_setup.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/testing/unittest
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/control_plane_proxy/events
    contrib/ydb/core/fq/libs/init
    contrib/ydb/core/fq/libs/mock
    contrib/ydb/core/testlib
    contrib/ydb/library/folder_service/mock
    contrib/ydb/library/grpc/server/actors
    contrib/ydb/library/security
    contrib/ydb/library/yql/providers/pq/provider
    contrib/ydb/tests/tools/kqprun/runlib
    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()
