LIBRARY()

SRCS(
    saver.cpp
    index_info.cpp
    loader.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/formats/arrow/transformer
    contrib/ydb/core/formats/arrow/serializer
)

YQL_LAST_ABI_VERSION()

END()
