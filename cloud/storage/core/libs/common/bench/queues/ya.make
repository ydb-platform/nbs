Y_BENCHMARK()
SIZE(MEDIUM)
TIMEOUT(600)

IF (SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
)

END()
