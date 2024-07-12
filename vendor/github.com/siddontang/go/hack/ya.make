GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

SRCS(
    hack.go
)

GO_TEST_SRCS(hack_test.go)

END()

RECURSE(
    gotest
)
