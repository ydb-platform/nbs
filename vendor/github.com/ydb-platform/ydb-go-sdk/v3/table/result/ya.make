GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v3.113.3)

SRCS(
    errors.go
    result.go
    set.go
)

END()

RECURSE(
    indexed
    named
)
