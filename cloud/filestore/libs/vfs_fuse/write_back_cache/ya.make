LIBRARY()

SRCS(
    calculate_data_parts_to_read.cpp
    session_sequencer.cpp
    write_back_cache.cpp
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
