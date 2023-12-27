LIBRARY()

SRCDIR(
    contrib/ydb/library/yql/minikql/codegen
)

ADDINCL(
    contrib/ydb/library/yql/minikql/codegen
)

SRCS(
    codegen_dummy.cpp
)

PROVIDES(MINIKQL_CODEGEN)

END()
