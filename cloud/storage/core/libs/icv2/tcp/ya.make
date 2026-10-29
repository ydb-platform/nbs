LIBRARY()

SRCS(
    connector.cpp
    listener.cpp
    socket_helper.cpp
    socket_worker.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/uds
    contrib/ydb/library/actors/interconnect
    library/cpp/threading/future
    library/cpp/time_provider
)

END()

RECURSE_FOR_TESTS(
    ut
)
