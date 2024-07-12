GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    client.go
    dial.go
    hybi.go
    server.go
    websocket.go
)

END()
