UNITTEST_FOR(contrib/ydb/core/scheme)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    contrib/ydb/core/scheme
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    scheme_borders_ut.cpp
    scheme_ranges_ut.cpp
    scheme_tablecell_ut.cpp
    scheme_types_proto_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
