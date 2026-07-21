LIBRARY()

SRCS(
    ut_common.cpp
)

PEERDIR(
    library/cpp/testing/unittest

    contrib/ydb/core/base

    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/common

    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics

    contrib/ydb/library/testlib/common
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/dq/common

    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/providers/common/schema/mkql
)

YQL_LAST_ABI_VERSION()

END()
