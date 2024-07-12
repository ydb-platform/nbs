GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    acl.go
    agent.go
    api.go
    catalog.go
    config_entry.go
    config_entry_discoverychain.go
    config_entry_exports.go
    config_entry_gateways.go
    config_entry_inline_certificate.go
    config_entry_intentions.go
    config_entry_mesh.go
    config_entry_routes.go
    config_entry_status.go
    connect.go
    connect_ca.go
    connect_intention.go
    coordinate.go
    debug.go
    discovery_chain.go
    event.go
    health.go
    kv.go
    lock.go
    namespace.go
    operator.go
    operator_area.go
    operator_autopilot.go
    operator_keyring.go
    operator_license.go
    operator_raft.go
    operator_segment.go
    operator_usage.go
    partition.go
    peering.go
    prepared_query.go
    raw.go
    semaphore.go
    session.go
    snapshot.go
    status.go
    txn.go
)

END()

RECURSE(
    # gotest
    watch
)
