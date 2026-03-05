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

PEERDIR(
    cloud/disk_manager/internal/pkg/clients/metrics
)

END()

RECURSE(
    config
    testing
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
