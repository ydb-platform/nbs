LIBRARY()

SRCS(
    yql_yt_fmr.cpp
)

PEERDIR(
    library/cpp/yson
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/mkql_simple_file
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/result/expr_nodes
    contrib/ydb/library/yql/utils/log
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/providers/yt/gateway/lib
    contrib/ydb/library/yql/providers/yt/gateway/native
    contrib/ydb/library/yql/providers/yt/expr_nodes
    contrib/ydb/library/yql/providers/yt/fmr/coordinator/interface
    contrib/ydb/library/yql/providers/yt/fmr/file/metadata/interface
    contrib/ydb/library/yql/providers/yt/fmr/file/upload/interface
    contrib/ydb/library/yql/providers/yt/fmr/job_launcher
    contrib/ydb/library/yql/providers/yt/fmr/job_preparer/interface
    contrib/ydb/library/yql/providers/yt/fmr/vanilla/coordinator_client
    contrib/ydb/library/yql/providers/yt/fmr/vanilla/peer_tracker
    contrib/ydb/library/yql/providers/yt/lib/config_clusters
    contrib/ydb/library/yql/providers/yt/lib/url_mapper
    contrib/ydb/library/yql/providers/yt/lib/res_pull
    contrib/ydb/library/yql/providers/yt/lib/schema
    contrib/ydb/library/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_yt_fmr.h)

END()
