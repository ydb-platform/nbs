LIBRARY()

LICENSE(
    BSD-2-Clause AND
    MIT
)

LICENSE_TEXTS(../LICENSE)

NO_UTIL()

SRCS(
    codec_plain.c
    lib.c
)

IF (OS_LINUX OR OS_DARWIN)
    CONLYFLAGS(-std=c11)
ENDIF()

END()
