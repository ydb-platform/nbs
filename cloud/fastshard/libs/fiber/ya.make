LIBRARY()

SRCS(
    event.cpp
    spawn.cpp
)

PEERDIR(
    cloud/fastshard/libs/common

    contrib/libs/silk/src/fibers
)

END()
