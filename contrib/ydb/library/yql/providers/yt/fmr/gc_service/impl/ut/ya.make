UNITTEST()

SRCS(
    yql_yt_gc_service_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/impl
    contrib/ydb/library/yql/providers/yt/fmr/gc_service/impl
    contrib/ydb/library/yql/providers/yt/fmr/table_data_service/local/impl
)

YQL_LAST_ABI_VERSION()

END()
