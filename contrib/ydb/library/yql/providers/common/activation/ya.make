LIBRARY()

SRCS(
    yql_activation.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/credentials
    contrib/ydb/library/yql/providers/common/proto
    library/cpp/svnversion
)

END()
