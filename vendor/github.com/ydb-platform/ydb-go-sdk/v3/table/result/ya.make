GO_LIBRARY()

LICENSE(Apache-2.0)

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
