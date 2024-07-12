GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    const.go
    doc.go
    doctype.go
    entity.go
    escape.go
    foreign.go
    node.go
    parse.go
    render.go
    token.go
)

END()

RECURSE(
    atom
    charset
)
