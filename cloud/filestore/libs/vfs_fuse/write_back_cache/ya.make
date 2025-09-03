LIBRARY()

SRCS(
    disjoint_interval_map.cpp
    overlapping_interval_set.cpp
    read_write_range_lock.cpp
    write_back_cache.cpp
    write_back_cache_util.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/threading/future
    library/cpp/threading/future/subscription
)

END()

RECURSE_FOR_TESTS(
    ut
)
