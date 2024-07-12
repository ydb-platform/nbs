GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    list.go
    table.go
)

GO_EMBED_PATTERN(data/children)

GO_EMBED_PATTERN(data/nodes)

GO_EMBED_PATTERN(data/text)

END()
