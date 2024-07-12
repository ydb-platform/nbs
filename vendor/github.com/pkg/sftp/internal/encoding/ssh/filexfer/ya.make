GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-2-Clause)

SRCS(
    attrs.go
    buffer.go
    extended_packets.go
    extensions.go
    filexfer.go
    fx.go
    fxp.go
    handle_packets.go
    init_packets.go
    open_packets.go
    packets.go
    path_packets.go
    permissions.go
    response_packets.go
)

GO_TEST_SRCS(
    attrs_test.go
    extended_packets_test.go
    extensions_test.go
    fx_test.go
    fxp_test.go
    handle_packets_test.go
    init_packets_test.go
    open_packets_test.go
    packets_test.go
    path_packets_test.go
    response_packets_test.go
)

END()

RECURSE(
    gotest
    openssh
)
