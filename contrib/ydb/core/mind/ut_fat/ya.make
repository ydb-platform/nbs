UNITTEST_FOR(contrib/ydb/core/mind)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/nodewarden
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    blobstorage_node_warden_ut_fat.cpp
)

END()
