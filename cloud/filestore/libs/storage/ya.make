RECURSE(
    api
    core
    fastshard
    init
    model
    perf
    service
    ss_proxy
    tablet
    tablet/model
    tablet/protos
    tablet_proxy
)

RECURSE_FOR_TESTS(
    testlib
)
