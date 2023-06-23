LIBRARY()

PEERDIR(
    library/cpp/threading/future
)

IF (OS_LINUX)
    PEERDIR(
        contrib/libs/libaio
    )
    SRCS(
        aio_linux.cpp
    )
ELSE(OS_LINUX)
    SRCS(
        aio_generic.cpp
    )
ENDIF(OS_LINUX)

END()
