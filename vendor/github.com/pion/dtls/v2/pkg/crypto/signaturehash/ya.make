GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    errors.go
    signaturehash.go
)

GO_TEST_SRCS(signaturehash_test.go)

END()

RECURSE(
    gotest
)
