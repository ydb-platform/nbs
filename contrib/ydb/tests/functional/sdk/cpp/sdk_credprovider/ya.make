UNITTEST()

ADDINCL(
    contrib/ydb/public/sdk/cpp
)

SRCS(
    dummy_provider_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/api/grpc
)


INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

END()
