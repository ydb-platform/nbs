LIBRARY()

CFLAGS(
    -Wno-deprecated-declarations
)

PEERDIR(
    library/cpp/containers/stack_vector
)

SRCS(
    cast.h
    dynumber.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
