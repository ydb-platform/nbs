LIBRARY()

SRCS(
    actors.cpp
    kqp_runner.cpp
    ydb_setup.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
