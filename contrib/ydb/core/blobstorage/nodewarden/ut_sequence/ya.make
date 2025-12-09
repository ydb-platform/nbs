UNITTEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/nodewarden
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/testlib/default
)

SRCS(
    dsproxy_config_retrieval.cpp
)

YQL_LAST_ABI_VERSION()

END()
