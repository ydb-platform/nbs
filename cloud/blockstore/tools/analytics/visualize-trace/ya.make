PY3_PROGRAM(blockstore-visualize-trace)

OWNER(g:cloud-nbs)

PEERDIR(
    cloud/blockstore/tools/analytics/find-perf-bottlenecks/lib
)

PY_SRCS(
    __main__.py
)

END()
