LIBRARY()

SRCS(
    ut_common.cpp
    ut_common.h
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/hooks/testing
    contrib/ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
