UNITTEST_FOR(contrib/ydb/core/tx/datashard)

PEERDIR(
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    export_s3_buffer_ut.cpp
)

END()
