GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    crypto.go
    math.go
)

GO_TEST_SRCS(
    crypto_test.go
    math_test.go
    rand_test.go
)

END()

RECURSE(
    gotest
)
