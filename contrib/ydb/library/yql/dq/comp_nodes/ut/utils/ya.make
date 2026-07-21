LIBRARY()


SRCS(
    utils.cpp
    dq_factories.cpp
)
PEERDIR(
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)
YQL_LAST_ABI_VERSION()

END()
