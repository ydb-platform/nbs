import yatest.common as common


def test_load():
    app = common.binary_path(
        "cloud/blockstore/tools/analytics/event-log-stats/blockstore-event-log-stats"
    )

    log = common.source_path(
        "cloud/blockstore/tools/analytics/event-log-stats/tests/data/profile.log.corrupt"
    )

    return common.canonical_execute(
        app,
        [
            "--evlog-dumper-params", log,
            "--disk-id", "ssd_v1"
        ]
    )
