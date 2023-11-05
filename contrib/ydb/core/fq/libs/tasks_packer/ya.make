LIBRARY()

SRCS(
    tasks_packer.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
