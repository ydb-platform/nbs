RECURSE(
    selftest

    local
    local-checkpoint
    local-discovery
    local-edgecase
    local-emergency
    local-encryption
    local-endpoints
    local-metrics
    local-mirror
    local-nemesis
    local-newfeatures
    local-nonrepl
    local-null
    local-overflow
    local-overlay
    local-endpoints-spdk
    local-throttling
    local-user-metrics
    local-v2
)

IF (NOT OPENSOURCE)
    RECURSE(
        local-auth              # TODO(NBS-4409): add to opensource
        local-change-device     # TODO(NBS-4409): add to opensource
    )
ENDIF()
