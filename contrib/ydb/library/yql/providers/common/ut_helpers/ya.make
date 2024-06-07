LIBRARY()

SRCS(
    dq_fake_ca.cpp
)

PEERDIR(
    library/cpp/retry
    contrib/ydb/core/testlib/basics
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/providers/common/comp_nodes
)

YQL_LAST_ABI_VERSION()

END()
