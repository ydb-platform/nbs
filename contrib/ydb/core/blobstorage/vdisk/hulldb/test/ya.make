LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/vdisk/hulldb/base
    contrib/ydb/core/blobstorage/vdisk/hulldb/generic
    contrib/ydb/core/protos
)

SRCS(
    defs.h
    testhull_index.h
    testhull_index.cpp
)

END()
