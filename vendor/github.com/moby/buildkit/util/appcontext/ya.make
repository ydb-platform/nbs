GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    appcontext.go
    register.go
)

IF (OS_LINUX)
    SRCS(
        appcontext_unix.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        appcontext_unix.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        appcontext_windows.go
    )
ENDIF()

END()
