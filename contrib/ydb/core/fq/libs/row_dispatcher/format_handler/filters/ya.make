LIBRARY()

SRCS(
    purecalc_filter.cpp
    filters_set.cpp
)

PEERDIR(
    contrib/libs/fmt

    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/row_dispatcher/events
    contrib/ydb/core/fq/libs/row_dispatcher/format_handler/common
    contrib/ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper

    contrib/ydb/library/actors/core

    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation
    contrib/ydb/library/yql/minikql/comp_nodes
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/providers/common/schema/parser
    contrib/ydb/library/yql/public/udf
)

YQL_LAST_ABI_VERSION()

END()
