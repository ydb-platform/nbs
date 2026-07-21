LIBRARY()

SRCS(
    context.cpp
    format.cpp
    fwd_backend.cpp
    log.cpp
    profile.cpp
    tls_backend.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/logger
    library/cpp/logger/global
    library/cpp/deprecated/atomic
    library/cpp/json
    contrib/ydb/library/yql/utils/log/proto
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/utils
)

END()

RECURSE_FOR_TESTS(
    ut
)
