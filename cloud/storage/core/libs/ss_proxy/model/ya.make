LIBRARY()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

SRCS(
    chunked_path_description_backup.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/ss_proxy/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
