GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    bugreport.go
    bugreport_mock.go
)

GO_TEST_SRCS(bugreport_test.go)

END()

RECURSE(
    gotest
)
