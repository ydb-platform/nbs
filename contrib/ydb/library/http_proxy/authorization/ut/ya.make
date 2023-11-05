UNITTEST_FOR(contrib/ydb/library/http_proxy/authorization)

PEERDIR(
    contrib/ydb/library/http_proxy/error
)

SRCS(
    auth_helpers_ut.cpp
    signature_ut.cpp
)

END()
