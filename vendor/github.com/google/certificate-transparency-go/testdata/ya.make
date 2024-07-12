GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    certs.go
    keys.go
    loglist3.go
)

GO_EMBED_PATTERN(loglist3.json)

END()
