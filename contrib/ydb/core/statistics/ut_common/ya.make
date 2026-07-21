LIBRARY()

SRCS(
    ut_common.cpp
    ut_common.h
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/testlib
    contrib/ydb/core/protos
    contrib/ydb/core/statistics
    contrib/ydb/core/statistics/common
)

YQL_LAST_ABI_VERSION()

END()
