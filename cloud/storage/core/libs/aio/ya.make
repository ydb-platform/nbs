LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    contrib/libs/libaio
)

END()

RECURSE_FOR_TESTS(ut)
