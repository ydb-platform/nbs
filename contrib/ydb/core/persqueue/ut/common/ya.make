LIBRARY()

SRCS(
    pq_ut_common.cpp
    pq_ut_common.h
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/core/persqueue
)

YQL_LAST_ABI_VERSION()

END()
