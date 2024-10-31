LIBRARY()

SRCS(
    auth_metadata.cpp
    channel_arguments.cpp
    completion.cpp
    credentials.cpp
    init.cpp
    keepalive.cpp
    request.cpp
    threadpool.cpp
    time_point_specialization.cpp
    utils.cpp
)

ADDINCL(
    contrib/libs/grpc
)

NO_COMPILER_WARNINGS()

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/deprecated/atomic
    library/cpp/grpc/common
    library/cpp/logger

    contrib/libs/grpc
    contrib/libs/grpc/src/proto/grpc/reflection/v1alpha
)

END()

RECURSE_FOR_TESTS(ut)
