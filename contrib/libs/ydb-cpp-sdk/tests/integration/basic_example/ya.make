GTEST()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)
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
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/driver
    contrib/libs/ydb-cpp-sdk/src/client/proto
    contrib/libs/ydb-cpp-sdk/src/client/table
)

SRCS(
    basic_example_data.cpp
    basic_example.cpp
    main.cpp
)

END()
