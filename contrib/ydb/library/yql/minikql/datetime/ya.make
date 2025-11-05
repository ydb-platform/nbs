LIBRARY()

SRCS(
    datetime.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/computation
)

YQL_LAST_ABI_VERSION()

END()
