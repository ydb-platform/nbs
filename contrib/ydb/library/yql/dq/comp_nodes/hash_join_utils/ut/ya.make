UNITTEST_FOR(contrib/ydb/library/yql/dq/comp_nodes/hash_join_utils)

IF (SANITIZER_TYPE OR NOT OPENSOURCE)
    REQUIREMENTS(ram:32 cpu:4)
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

IF (ARCH_X86_64 AND OS_LINUX)
SRCS(
    accumulator_ut.cpp
    scalar_layout_converter_ut.cpp
    scalar_layout_converter_test_enums.h
    block_layout_converter_ut.cpp
    block_layout_converter_sliced_blocks_ut.cpp
    hash_table_ut.cpp
    packed_tuple_ut.cpp
    deep_copy_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/sql/pg_dummy
)

CFLAGS(
    -mavx2
    -mprfchw
)

ENDIF()

GENERATE_ENUM_SERIALIZATION(scalar_layout_converter_test_enums.h)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
