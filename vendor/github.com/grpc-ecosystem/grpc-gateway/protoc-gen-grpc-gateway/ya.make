GO_PROGRAM()

LICENSE(BSD-3-Clause)

VERSION(v1.16.0)

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
