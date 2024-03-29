LIBRARY()

SRCS(
    connector_client_mock.cpp
    database_resolver_mock.cpp
    defaults.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/testing/gmock_in_unittest
    library/cpp/testing/unittest
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/common/structured_token
    contrib/ydb/library/yql/providers/generic/connector/api/common
    contrib/ydb/library/yql/providers/generic/connector/libcpp
)

YQL_LAST_ABI_VERSION()

END()
