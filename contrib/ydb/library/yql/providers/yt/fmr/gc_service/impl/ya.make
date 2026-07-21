LIBRARY()

SRCS(
    yql_yt_gc_service_impl.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/gc_service/interface
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
