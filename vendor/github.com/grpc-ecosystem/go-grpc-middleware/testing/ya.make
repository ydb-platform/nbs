GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    interceptor_suite.go
    mutex_readerwriter.go
    pingservice.go
)

END()

RECURSE(
    gogotestproto
    testproto
)
