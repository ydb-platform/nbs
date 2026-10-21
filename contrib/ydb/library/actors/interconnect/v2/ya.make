LIBRARY()

SRCS(
    interconnect_direct_session.h
    interconnect_uring_engine.h
    interconnect_uring_event_queue.h
    v2_chunk_serializer.cpp
    v2_chunk_serializer.h
    v2_event_serializer.cpp
    v2_event_serializer.h
    v2_io_buffers.h
    v2_serialize_window.h
)

IF (OS_LINUX)
    SRCS(
        uring_context.cpp
        uring_context.h
        interconnect_uring_engine.cpp
    )
ELSE()
    SRCS(
        interconnect_uring_engine_stub.cpp
    )
ENDIF()

PEERDIR(
    contrib/libs/xxhash
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/v2/protos
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/wilson
    contrib/ydb/library/uring
    library/cpp/monlib/dynamic_counters
    library/cpp/monlib/service/pages
)

END()

RECURSE(
    protos
)

RECURSE_FOR_TESTS(
    ut
)
