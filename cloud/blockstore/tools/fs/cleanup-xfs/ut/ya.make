UNITTEST_FOR(cloud/blockstore/tools/fs/cleanup-xfs)

SRCS(
    cleanup_ut.cpp
    cleanup.cpp

    parser_ut.cpp
    parser.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

DATA(
    arcadia/cloud/blockstore/tools/fs/cleanup-xfs/ut/sb.txt
    arcadia/cloud/blockstore/tools/fs/cleanup-xfs/ut/freesp.txt
)

END()
