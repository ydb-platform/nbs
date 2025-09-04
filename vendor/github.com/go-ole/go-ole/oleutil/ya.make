GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.2.7-0.20211215081658-ee6c8cce8e87)

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
