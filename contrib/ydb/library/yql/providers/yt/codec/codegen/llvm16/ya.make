LIBRARY()

SRCDIR(contrib/ydb/library/yql/providers/yt/codec/codegen/llvm16)

PEERDIR(
    contrib/ydb/library/yql/minikql/codegen/llvm16
)

USE_LLVM_BC16()
SET(LLVM_VER 16)

# mcd/dc coverage is introduced in Clang18


INCLUDE(../ya.make.inc)

END()
