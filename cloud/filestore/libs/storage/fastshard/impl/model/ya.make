LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    handle_table.cpp
    helpers.cpp
    name_table.cpp
    node_table.cpp
    page_store.cpp
    persistent_bitmap.cpp
    persistent_hash_table.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/ipc

    cloud/storage/core/libs/common

    contrib/libs/silk/src/fibers

    library/cpp/json
)

PEERDIR(
    cloud/filestore/libs/service
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/sn/quorum
    cloud/filestore/libs/storage/model

    cloud/filestore/public/api/protos
)

END()
