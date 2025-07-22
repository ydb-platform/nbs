LIBRARY()

SRCS(
    context.cpp
    factory.cpp
    service.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    contrib/libs/liburing
)

END()

RECURSE_FOR_TESTS(ut)
