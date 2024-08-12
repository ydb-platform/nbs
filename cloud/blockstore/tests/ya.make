# DEVTOOLSSUPPORT-18977
IF (SANITIZER_TYPE != "undefined" AND SANITIZER_TYPE != "memory")
    RECURSE(
        plugin
    )
ENDIF()

RECURSE(
    client
    cms
    csi_driver
    external_endpoint
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
    resize-disk
    secure-registration
    session_cache
    spare_node
    stats_aggregator_perf
    storage_discovery
    vhost-server
    vhost-server/run_and_die
)
