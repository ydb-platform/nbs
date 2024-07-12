GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    catalog_gen.go
    main.go
)

END()

RECURSE(
    pkg
)
