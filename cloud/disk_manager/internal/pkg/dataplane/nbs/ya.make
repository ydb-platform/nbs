OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    common.go
    source.go
    target.go
)

END()

RECURSE_FOR_TESTS(
    tests
)
