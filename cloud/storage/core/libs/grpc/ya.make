LIBRARY()

SRCS(
    completion.cpp
    credentials.cpp
    initializer.cpp
    keepalive.cpp
    logging.cpp
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

    library/cpp/deprecated/atomic
    library/cpp/grpc/common
    library/cpp/logger

    contrib/libs/grpc
    contrib/libs/grpc/src/proto/grpc/reflection/v1alpha
)

END()

RECURSE_FOR_TESTS(ut)
