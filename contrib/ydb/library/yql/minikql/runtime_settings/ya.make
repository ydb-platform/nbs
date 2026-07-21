LIBRARY()

SRCS(
    runtime_settings_configuration.cpp
    runtime_settings_hash.cpp
    runtime_settings_serialization.cpp
    runtime_settings.cpp
)

PEERDIR(
    contrib/ydb/library/yql/minikql/runtime_settings/proto
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/activation
    contrib/libs/openssl
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
