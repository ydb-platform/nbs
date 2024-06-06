GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    gen.go
    rawhelpers.go
)

END()

RECURSE(
    attribute
    baggage
    global
    internaltest
    matchers
)
