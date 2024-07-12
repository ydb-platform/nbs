GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    encode.go
    hpack.go
    huffman.go
    static_table.go
    tables.go
)

END()
