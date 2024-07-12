GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    generator_interceptor.go
    generator_option.go
    pli.go
)

GO_TEST_SRCS(generator_interceptor_test.go)

END()

RECURSE(
    gotest
)
