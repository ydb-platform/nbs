GO_LIBRARY()

SET(
    GO_VET_FLAGS
    -printf=false
)

SRCS(
    client.go
    endpoint_picker.go
    interface.go
    factory.go
    session.go
)

END()

RECURSE(
    config
    protos
    testing
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
