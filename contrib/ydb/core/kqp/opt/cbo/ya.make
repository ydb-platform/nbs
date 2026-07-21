LIBRARY()

SRCS(
    cbo_hints.cpp
    cbo_interesting_orderings.cpp
    cbo_optimizer_hints.cpp
    cbo_optimizer_new.cpp
    kqp_statistics.cpp
)

GENERATE_ENUM_SERIALIZATION(cbo_optimizer_new.h)

PEERDIR(
    library/cpp/disjoint_sets
    library/cpp/iterator
    library/cpp/json
    library/cpp/string_utils/base64
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/minsketch
    contrib/ydb/library/yql/core/histogram
    contrib/ydb/library/yql/utils/log
)

CHECK_DEPENDENT_DIRS(DENY PEERDIRS
    contrib/ydb/library/yql/core/cbo
)

YQL_LAST_ABI_VERSION()

END()
