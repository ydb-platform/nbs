RECURSE(
    libs

    block-data-dump
    calculate-perf-settings
    compaction-sim
    dump-event-log
    event-log-disk-usage
    event-log-stats
    event-log-suffer
    find-block-accesses
    visualize-event-log
)

IF (NOT OPENSOURCE)
    RECURSE(
        find-perf-bottlenecks   # TODO(NBS-4409): add to opensource
        visualize-trace         # TODO(NBS-4409): add to opensource
    )
ENDIF()

