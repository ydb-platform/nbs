UNITTEST()

SRCS(
    dummy_provider_ut.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_scheme
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/api/grpc
)


INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

REQUIREMENTS(ram:16)

END()
