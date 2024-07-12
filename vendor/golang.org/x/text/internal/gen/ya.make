GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    code.go
    gen.go
)

END()

RECURSE(
    bitfield
)
