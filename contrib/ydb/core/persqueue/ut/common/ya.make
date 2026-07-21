LIBRARY()

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    pq_ut_common.cpp
    pq_ut_common.h

    autoscaling_ut_common.cpp
    autoscaling_ut_common.h

    sdk_ut_common.cpp
    sdk_ut_common.h
)

PEERDIR(
    contrib/ydb/core/persqueue
    contrib/ydb/core/persqueue/public/schema
    contrib/ydb/core/testlib
    contrib/ydb/public/sdk/cpp/src/client/topic
)

YQL_LAST_ABI_VERSION()

END()
