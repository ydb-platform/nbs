GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    gen.go
)

GO_EMBED_PATTERN(templates/*)

END()
