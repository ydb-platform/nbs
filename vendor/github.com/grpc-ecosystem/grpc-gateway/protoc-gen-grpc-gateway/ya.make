GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    main.go
)

END()

RECURSE(
    descriptor
    generator
    httprule
    internal
)
