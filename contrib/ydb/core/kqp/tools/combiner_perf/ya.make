LIBRARY()

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy

    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/minikql/codegen/llvm16
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16

    library/cpp/testing/unittest

    contrib/ydb/core/kqp/runtime

    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/comp_nodes/ut/utils

    contrib/libs/llvm16/lib/IR
    contrib/libs/llvm16/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm16/lib/Linker
    contrib/libs/llvm16/lib/Passes
    contrib/libs/llvm16/lib/Target/X86
    contrib/libs/llvm16/lib/Target/X86/AsmParser
    contrib/libs/llvm16/lib/Target/X86/Disassembler
    contrib/libs/llvm16/lib/Transforms/IPO
    contrib/libs/llvm16/lib/Transforms/ObjCARC
)

IF (ARCH_X86_64)

CFLAGS(
    -mprfchw
)

ENDIF()

SRCS(
    converters.cpp
    dq_combine_vs.cpp
    factories.cpp
    printout.cpp
    simple.cpp
    simple_block.cpp
    simple_grace_join.cpp
    simple_last.cpp
    subprocess.cpp
    streams.cpp
    tpch_last.cpp
    fs_utils.cpp
)

END()
