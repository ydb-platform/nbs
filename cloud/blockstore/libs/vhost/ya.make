LIBRARY()

SRCS(
    server.cpp
    vhost.cpp
    vhost_test.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service

    cloud/contrib/vhost
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_stress
)
