GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    defaults.go
)

GO_EMBED_PATTERN(editions_defaults.binpb)

END()
