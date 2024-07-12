GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    fingerprint.go
    hash.go
)

GO_TEST_SRCS(
    fingerprint_test.go
    hash_test.go
)

END()

RECURSE(
    gotest
)
