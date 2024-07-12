GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    svgpan.go
)

GO_EMBED_PATTERN(svgpan.js)

END()
