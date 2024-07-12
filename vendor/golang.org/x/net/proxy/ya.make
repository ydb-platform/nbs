GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    dial.go
    direct.go
    per_host.go
    proxy.go
    socks5.go
)

END()
