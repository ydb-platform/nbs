LIBRARY()

SRCS(
    block_buffer.cpp
    block_checksum.cpp
    block_data_ref.cpp
    block_range.cpp
    caching_allocator.cpp
    guarded_sglist.cpp
    iovector.cpp
    sglist.cpp
    sglist_test.cpp
    typeinfo.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos
    cloud/storage/core/libs/common
    contrib/ydb/library/actors/prof
    library/cpp/digest/crc32c
    library/cpp/threading/future
    library/cpp/deprecated/atomic
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
