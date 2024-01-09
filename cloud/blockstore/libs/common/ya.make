LIBRARY()

SRCS(
    block_checksum.cpp
    block_range.cpp
    caching_allocator.cpp
    device_path.cpp
    iovector.cpp
    typeinfo.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos

    cloud/storage/core/libs/common

    library/cpp/digest/crc32c
    library/cpp/threading/future
    library/cpp/deprecated/atomic

    library/cpp/actors/prof
)

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(
        -DPROFILE_MEMORY_ALLOCATIONS
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
