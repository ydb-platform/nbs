# DEVTOOLSSUPPORT-18977
IF (SANITIZER_TYPE != "undefined" AND SANITIZER_TYPE != "memory")
    RECURSE(
        plugin
    )
ENDIF()

RECURSE(
    client
    config_dispatcher
    csi_driver
    disk_agent_config
    encryption_at_rest
    external_endpoint
    e2e-tests
    fio
    functional
    fuzzing
    infra-cms
    loadtest
    loadtest/remote
    local_ssd
    monitoring
    mount
    notify
    python
    python_client
    rdma
    recipes
    registration
    resize-disk
    session_cache
    spare_node
    stats_aggregator_perf
    storage_discovery
    vhost-server
    vhost-server/run_and_die
)
