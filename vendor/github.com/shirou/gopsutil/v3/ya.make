GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
)

END()

RECURSE(
    common
    cpu
    disk
    docker
    host
    internal
    load
    mem
    net
    process
)

IF (OS_WINDOWS)
    RECURSE(
        winservices
    )
ENDIF()
