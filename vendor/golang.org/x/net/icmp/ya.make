GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    dstunreach.go
    echo.go
    endpoint.go
    extension.go
    helper_posix.go
    interface.go
    ipv4.go
    ipv6.go
    listen_posix.go
    message.go
    messagebody.go
    mpls.go
    multipart.go
    packettoobig.go
    paramprob.go
    timeexceeded.go
)

END()
