UNITTEST()

ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/ydbd_dep.inc)

DEPENDS(
    contrib/ydb/public/tools/ydb_recipe
    contrib/ydb/library/yql/udfs/common/datetime
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/pire
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/string
)

SRCS(
    kqp_tpch_ut.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/tests/tpch/lib
    library/cpp/testing/unittest
    contrib/ydb/core/protos
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    yql/essentials/public/udf/service/stub
    contrib/ydb/public/lib/yson_value
)

SIZE(MEDIUM)

END()
