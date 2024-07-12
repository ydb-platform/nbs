SUBSCRIBER(g:go-contrib)

IF (OS_LINUX)
    RECURSE(
        pty
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        pty
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        pty
    )
ENDIF()
