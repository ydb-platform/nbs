GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    clock.go
    doc.go
    timeout.go
    writer.go
)

GO_TEST_SRCS(clock_test.go)

END()

RECURSE(
    gotest
)
