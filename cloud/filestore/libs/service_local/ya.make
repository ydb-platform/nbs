LIBRARY()

SRCS(
    cluster.cpp
    config.cpp
    fs.cpp
    fs_attrs.cpp
    fs_cluster.cpp
    fs_data.cpp
    fs_lock.cpp
    fs_node.cpp
    fs_session.cpp
    index.cpp
    service.cpp
    session.cpp
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/libcap
    )

    SRCS(
        lowlevel.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/protobuf/util

    contrib/libs/protobuf
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_stress
)
