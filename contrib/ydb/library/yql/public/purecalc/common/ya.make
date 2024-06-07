LIBRARY()

INCLUDE(ya.make.inc)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/codec/codegen
    contrib/ydb/library/yql/minikql/computation/llvm14
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/ydb/library/yql/minikql/comp_nodes/llvm14
)

END()

RECURSE(
    no_llvm
)

