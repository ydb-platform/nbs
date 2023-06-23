LIBRARY()

IF (NOT NO_UTIL_SVN_DEPEND)
    PEERDIR(
        library/cpp/svnversion
    )
ELSE()
    CFLAGS(-DNO_SVN_DEPEND)
ENDIF()

PEERDIR(
    contrib/libs/re2
    library/cpp/getopt/small
    library/cpp/eventlog
)

SRCS(
    common.cpp
    evlogdump.cpp
    tunable_event_processor.cpp
)

END()
