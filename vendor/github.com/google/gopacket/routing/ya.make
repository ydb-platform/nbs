GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    common.go
)

IF (OS_LINUX)
    SRCS(
        routing.go
    )
ENDIF()

IF (OS_DARWIN)
    SRCS(
        other.go
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        other.go
    )
ENDIF()

END()
