OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    client.go
    interface.go
    factory.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    mocks
)
