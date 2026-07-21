UNITTEST_FOR(contrib/ydb/core/persqueue/pqtablet/blob)

SRCS(
    blob_ut.cpp
    type_codecs_ut.cpp
)

PEERDIR (
    contrib/ydb/core/persqueue/common
)

END()
