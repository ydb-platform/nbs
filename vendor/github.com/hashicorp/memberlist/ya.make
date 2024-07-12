GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    alive_delegate.go
    awareness.go
    broadcast.go
    config.go
    conflict_delegate.go
    delegate.go
    event_delegate.go
    keyring.go
    label.go
    logging.go
    memberlist.go
    merge_delegate.go
    mock_transport.go
    net.go
    net_transport.go
    peeked_conn.go
    ping_delegate.go
    queue.go
    security.go
    state.go
    suspicion.go
    transport.go
    util.go
)

GO_TEST_SRCS(
    awareness_test.go
    broadcast_test.go
    config_test.go
    integ_test.go
    keyring_test.go
    label_test.go
    logging_test.go
    memberlist_test.go
    net_test.go
    queue_test.go
    security_test.go
    state_test.go
    suspicion_test.go
    transport_test.go
    util_test.go
)

END()

RECURSE(
    gotest
    internal
)
