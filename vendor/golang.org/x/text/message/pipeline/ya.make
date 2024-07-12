GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    extract.go
    generate.go
    message.go
    pipeline.go
    rewrite.go
)

GO_TEST_SRCS(
    go19_test.go
    pipeline_test.go
)

END()

RECURSE(
    # gotest
)
