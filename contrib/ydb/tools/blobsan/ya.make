PROGRAM()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/streams/bzip2
    contrib/ydb/core/blobstorage
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/vdisk/query
)

END()
