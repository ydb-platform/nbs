LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    aliases.h
    common.h
    common.cpp
    persqueue_impl.h
    persqueue_impl.cpp
    persqueue.cpp
    read_session.h
    read_session.cpp
    read_session_messages.cpp
    write_session_impl.h
    write_session_impl.cpp
    write_session.h
    write_session.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/metrics
    library/cpp/string_utils/url
    library/cpp/containers/disjoint_interval_tree
    contrib/libs/ydb-cpp-sdk/src/library/grpc/client
    contrib/libs/ydb-cpp-sdk/src/library/persqueue/obfuscate
    contrib/ydb/public/api/grpc/draft
    contrib/libs/ydb-cpp-sdk/src/client/impl/ydb_internal/make_request
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/topic/codecs
    contrib/libs/ydb-cpp-sdk/src/client/topic/common
    contrib/libs/ydb-cpp-sdk/src/client/topic/impl

)

END()
