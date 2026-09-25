LIBRARY()

SRCDIR(contrib/ydb/core/tx/columnshard/engines/scheme/indexes/abstract)

SRCS(
    fetcher.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
)

YQL_LAST_ABI_VERSION()

END()
