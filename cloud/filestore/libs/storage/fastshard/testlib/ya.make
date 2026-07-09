LIBRARY()

SRCS(
    fake_storage_node.cpp
    silk_env.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/sn/iface

    cloud/storage/core/protos

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
