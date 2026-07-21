LIBRARY()

SRCS(
    yql_dispatch.cpp
    yql_setting.h
)

PEERDIR(
    contrib/ydb/library/yql/core/qplayer/storage/interface
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/ast
    library/cpp/containers/sorted_vector
    library/cpp/string_utils/parse_size
    library/cpp/string_utils/levenshtein_diff
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(transformer)

RECURSE_FOR_TESTS(ut)
