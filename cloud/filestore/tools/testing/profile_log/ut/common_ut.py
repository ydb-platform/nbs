from unittest import mock

import cloud.filestore.tools.testing.profile_log.common as profile_log


def _event_line(request_type, *body_parts):
    return "\t".join([
        "1970-01-01T00:00:00.000010Z",
        "nfs_test",
        request_type,
        "0.000020s",
        "S_OK",
        *body_parts,
    ]) + "\n"


class _ExecutionResult:
    @property
    def stdout(self):
        raise AssertionError("dumpevents stdout must not be materialized")


def _execute_writer(dump):
    streams = []

    def execute(command, **kwargs):
        output = kwargs.get("stdout")
        assert output is not None

        # Writing bytes verifies that the caller supplied a binary file rather
        # than asking common.execute to capture stdout in memory.
        output.write(dump.encode("utf-8"))
        output.flush()
        streams.append(output)
        return _ExecutionResult()

    return execute, streams


def test_iter_profile_log_events_uses_caller_owned_binary_stdout():
    dump = _event_line(
        "ReadData",
        "{node_id=42, handle=3}",
        "[{offset=4096, bytes=8192}]",
        "loop_thread_id=17",
        "client_id=test-client",
    )
    execute, streams = _execute_writer(dump)

    with mock.patch.object(
        profile_log.common,
        "execute",
        side_effect=execute,
    ) as execute_mock:
        events = list(profile_log.iter_profile_log_events(
            "/test/filestore-profile-tool",
            "/test/profile.log",
            "nfs_test",
        ))

    assert events == [(
        "ReadData",
        {
            "node_id": "42",
            "handle": "3",
            "offset": "4096",
            "bytes": "8192",
            "loop_thread_id": "17",
            "client_id": "test-client",
        },
    )]
    execute_mock.assert_called_once()
    assert execute_mock.call_args.args[0] == [
        "/test/filestore-profile-tool",
        "dumpevents",
        "--profile-log",
        "/test/profile.log",
        "--fs-id",
        "nfs_test",
    ]
    assert execute_mock.call_args.kwargs["stdout"] is streams[0]
    assert streams[0].closed


def test_iter_profile_log_events_parses_incrementally():
    lines = [
        _event_line("ReadData", "loop_thread_id=1"),
        _event_line("WriteData", "loop_thread_id=2"),
    ]
    consumed = []

    def iter_lines(*_args, **_kwargs):
        for line in lines:
            consumed.append(line)
            yield line

    with mock.patch.object(
        profile_log,
        "_iter_profile_log_lines",
        side_effect=iter_lines,
    ):
        events = profile_log.iter_profile_log_events(
            "/test/filestore-profile-tool",
            "/test/profile.log",
            "nfs_test",
        )

        assert iter(events) is events
        assert consumed == []
        assert next(events) == ("ReadData", {"loop_thread_id": "1"})
        assert consumed == [lines[0]]
        assert list(events) == [("WriteData", {"loop_thread_id": "2"})]


def test_get_profile_log_events_remains_a_list_wrapper():
    expected = [
        ("ReadData", {"loop_thread_id": "1"}),
        ("WriteData", {"loop_thread_id": "2"}),
    ]

    with mock.patch.object(
        profile_log,
        "iter_profile_log_events",
        return_value=iter(expected),
    ) as iter_events:
        events = profile_log.get_profile_log_events(
            "/test/filestore-profile-tool",
            "/test/profile.log",
            "nfs_test",
        )

    assert isinstance(events, list)
    assert events == expected
    iter_events.assert_called_once_with(
        "/test/filestore-profile-tool",
        "/test/profile.log",
        "nfs_test",
    )


def test_analyze_profile_log_filters_by_node_name():
    dump = "".join([
        _event_line("ReadData", "{node_name=selected, node_id=1}"),
        _event_line("WriteData", "{node_name=other, node_id=2}"),
        _event_line("ReadData", "{node_name=selected, node_id=3}"),
    ])
    execute, _ = _execute_writer(dump)

    with mock.patch.object(
        profile_log.common,
        "execute",
        side_effect=execute,
    ):
        counts = profile_log.analyze_profile_log(
            "/test/filestore-profile-tool",
            "/test/profile.log",
            "nfs_test",
            node_name_filter="selected",
        )

    assert counts == {"ReadData": 2}
