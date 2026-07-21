LIBRARY()

SRCS(
    url_lister_manager.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core/url_lister/interface
)

END()

RECURSE(
    interface
)

