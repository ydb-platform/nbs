GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    context.go
    init.go
    martian.go
    multierror.go
    noop.go
    proxy.go
)

GO_TEST_SRCS(
    context_test.go
    martian_test.go
    multierror_test.go
    proxy_test.go
    proxy_trafficshaping_test.go
)

END()

RECURSE(
    cybervillains
    fifo
    filter
    gotest
    h2
    header
    httpspec
    log
    martianhttp
    martianlog
    martiantest
    messageview
    mitm
    parse
    proxyutil
    trafficshape
    verify
)

IF (OS_LINUX)
    RECURSE(
        nosigpipe
    )
ENDIF()

IF (OS_DARWIN)
    RECURSE(
        nosigpipe
    )
ENDIF()

IF (OS_WINDOWS)
    RECURSE(
        nosigpipe
    )
ENDIF()
