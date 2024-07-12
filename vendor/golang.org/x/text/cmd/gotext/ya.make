GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    common.go
    doc.go
    extract.go
    generate.go
    main.go
    rewrite.go
    update.go
)

END()

RECURSE(
    examples
)
