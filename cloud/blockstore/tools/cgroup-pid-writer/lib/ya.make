LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    app.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    library/cpp/getopt
)

END()

RECURSE_FOR_TESTS(
    ut
)
