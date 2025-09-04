LIBRARY()

CFLAGS(
    -Wno-deprecated-declarations
)

SRCS(
    log.cpp
)

PEERDIR(
    library/cpp/logger
)

END()
