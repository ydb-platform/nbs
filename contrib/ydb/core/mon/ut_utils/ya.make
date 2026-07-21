LIBRARY()

SRCS(
    ut_utils.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/library/aclib
    contrib/ydb/library/security
)

YQL_LAST_ABI_VERSION()

END()
