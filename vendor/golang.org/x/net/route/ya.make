GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

IF (OS_DARWIN)
    SRCS(
        address.go
        binary.go
        empty.s
        interface.go
        interface_classic.go
        interface_multicast.go
        message.go
        route.go
        route_classic.go
        sys.go
        sys_darwin.go
        syscall.go
        zsys_darwin.go
    )
ENDIF()

END()
