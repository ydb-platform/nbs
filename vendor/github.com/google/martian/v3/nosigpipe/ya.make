GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

IF (OS_LINUX)
    SRCS(
        nosigpipe.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        nosigpipe_darwin.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        nosigpipe.go
    )
ENDIF()

END()
