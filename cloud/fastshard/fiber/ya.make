LIBRARY()

SRCS(
    event.cpp
    spawn.cpp
)

PEERDIR(
    cloud/fastshard/common

    contrib/libs/silk/src/fibers
)

END()
