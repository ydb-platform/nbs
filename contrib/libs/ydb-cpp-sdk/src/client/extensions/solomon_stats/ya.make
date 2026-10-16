LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    pull_client.cpp
    pull_connector.cpp
)

PEERDIR(
    library/cpp/monlib/encode/json
    library/cpp/monlib/metrics
    library/cpp/monlib/service
    library/cpp/monlib/service/pages
    contrib/libs/ydb-cpp-sdk/src/client/extension_common
)

END()
