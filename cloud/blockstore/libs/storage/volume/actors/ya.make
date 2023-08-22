LIBRARY()

SRCS(
    forward_read_marked.cpp
    forward_write_and_mark_used.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb

    library/cpp/actors/core
    library/cpp/lwtrace
)

END()

RECURSE()

RECURSE_FOR_TESTS(
    ut
)
