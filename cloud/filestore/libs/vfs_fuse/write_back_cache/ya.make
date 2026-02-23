LIBRARY()

SRCS(
    disjoint_interval_builder.cpp
    node_cache.cpp
    node_state_holder.cpp
    overlapping_interval_set.cpp
    persistent_storage.cpp
    read_write_range_lock.cpp
    sequence_id_generator.cpp
    utils.cpp
    write_back_cache.cpp
    write_back_cache_stats.cpp
    write_data_request_builder.cpp
    write_data_request_manager.cpp
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
