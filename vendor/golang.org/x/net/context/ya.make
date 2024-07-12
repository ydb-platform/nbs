GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    context.go
    go17.go
    go19.go
)

END()

RECURSE(
    ctxhttp
)
