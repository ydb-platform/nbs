LIBRARY()

SRCS(
    stable_pickle.cpp
    stable_pickle.h
)

PEERDIR(
    util
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/public/lib/scheme_types
)

YQL_LAST_ABI_VERSION()

END()
