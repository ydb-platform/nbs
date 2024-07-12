GO_PROGRAM()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    pprof.go
)

END()

RECURSE(
    driver
    fuzz
    internal
    profile
    third_party
)
