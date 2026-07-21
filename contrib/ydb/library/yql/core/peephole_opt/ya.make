LIBRARY()

ENABLE(SKIP_YQL_STYLE_CPP)

SRCS(
    yql_opt_json_peephole_physical.h
    yql_opt_json_peephole_physical.cpp
    yql_opt_peephole_physical.h
    yql_opt_peephole_physical.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/common_opt
    contrib/ydb/library/yql/core/type_ann
    library/cpp/svnversion
)

YQL_LAST_ABI_VERSION()

END()
