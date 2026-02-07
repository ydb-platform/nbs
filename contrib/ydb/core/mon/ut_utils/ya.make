LIBRARY()

SRCS(
    ut_utils.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
)

YQL_LAST_ABI_VERSION()

END()
