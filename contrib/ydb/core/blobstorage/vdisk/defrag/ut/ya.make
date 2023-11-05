UNITTEST_FOR(contrib/ydb/core/blobstorage/vdisk/defrag)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/core/blobstorage/vdisk/defrag
    contrib/ydb/core/blobstorage/vdisk/hulldb
)

SRCS(
    defrag_actor_ut.cpp
)

END()
