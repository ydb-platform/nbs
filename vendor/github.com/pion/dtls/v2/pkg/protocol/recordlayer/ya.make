GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    errors.go
    header.go
    recordlayer.go
)

GO_TEST_SRCS(
    fuzz_test.go
    recordlayer_test.go
)

END()

RECURSE(
    gotest
)
