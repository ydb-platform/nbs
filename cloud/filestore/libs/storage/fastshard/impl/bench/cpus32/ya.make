G_BENCHMARK()

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    registration.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/impl/bench/lib
)

END()
