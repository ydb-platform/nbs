SUBSCRIBER(g:go-contrib)

IF (OS_DARWIN OR OS_LINUX)
    RECURSE(
        unix
    )
ENDIF()

RECURSE(
    cpu
    execabs
    # unix
)

IF (OS_WINDOWS)
    RECURSE(
        windows
    )
ENDIF()
