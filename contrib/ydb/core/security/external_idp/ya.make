LIBRARY()

SRCS(
    external_idp_provider.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    contrib/libs/openssl
    library/cpp/json
    library/cpp/html/pcdata
    library/cpp/string_utils/base64
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/security/util
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    contrib/ydb/library/services
)

GENERATE_ENUM_SERIALIZATION(external_idp_provider.h)

END()

RECURSE_FOR_TESTS(
    ut
)
