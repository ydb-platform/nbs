GTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/libs/grpc
    contrib/ydb/public/api/grpc
    contrib/ydb/public/sdk/cpp/src/client/query
)

SRCS(
    main.cpp
)

END()
