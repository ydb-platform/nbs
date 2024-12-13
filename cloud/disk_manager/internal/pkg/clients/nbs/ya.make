GO_LIBRARY()

SRCS(
    client.go
    disk_registry_state.go
    factory.go
    interface.go
    metrics.go
    multi_zone_client.go
    session.go
    testing_client.go
)

END()

RECURSE(
    config
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
