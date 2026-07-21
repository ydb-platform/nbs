UNITTEST_FOR(contrib/ydb/core/security/external_idp)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/security/external_idp/test_utils
    contrib/ydb/core/util/actorsys_test
    contrib/ydb/library/actors/http
    contrib/ydb/library/services
    library/cpp/json
    library/cpp/monlib/service/pages
    library/cpp/string_utils/base64
    library/cpp/testing/unittest
    contrib/libs/jwt-cpp
    contrib/libs/openssl
)

SRCS(
    external_idp_provider_ut.cpp
)

END()
