GO_LIBRARY()

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
