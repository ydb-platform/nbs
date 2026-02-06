UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/anubis_osiris)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/base
)

SRCS(
    blobstorage_anubis_algo_ut.cpp
)

END()
