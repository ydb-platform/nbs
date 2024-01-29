LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/deny_ydb_dependency.inc)

SRCS(
    config.cpp
)

PEERDIR(
    cloud/blockstore/config
    library/cpp/monlib/service/pages
)

END()
