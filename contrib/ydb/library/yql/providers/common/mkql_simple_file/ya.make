LIBRARY()

SRCS(
    mkql_simple_file.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/minikql
)

YQL_LAST_ABI_VERSION()

END()
