GO_LIBRARY()

SRCS(
    client.go
    endpoint_picker.go
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
