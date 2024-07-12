GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    oleutil.go
)

IF (OS_LINUX)
    SRCS(
        connection_func.go
        go-get.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        connection_func.go
        go-get.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        connection.go
        connection_windows.go
    )
ENDIF()

END()
