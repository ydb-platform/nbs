GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    doc.go
)

END()

RECURSE(
    bidi
    cldr
    norm
    rangetable
    runenames
)
