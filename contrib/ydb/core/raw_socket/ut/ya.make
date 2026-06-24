UNITTEST_FOR(contrib/ydb/core/raw_socket)

SIZE(small)
SRCS(
    buffered_writer_ut.cpp
)

PEERDIR(
    contrib/ydb/core/raw_socket
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()
END()
