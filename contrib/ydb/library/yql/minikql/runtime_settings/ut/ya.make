UNITTEST_FOR(contrib/ydb/library/yql/minikql/runtime_settings)

SRCS(
    runtime_settings_hash_ut.cpp
    runtime_settings_serialization_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/core/credentials
    contrib/ydb/library/yql/core/qplayer/storage/memory
    contrib/ydb/library/yql/providers/common/activation
)

YQL_LAST_ABI_VERSION()

END()
