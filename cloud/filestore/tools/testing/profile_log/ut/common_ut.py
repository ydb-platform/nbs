from unittest import mock

import pytest

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
    if isinstance(dump, str):
        dump = dump.encode("utf-8")

    def execute(_command, **kwargs):
        output = kwargs.get("stdout")
        assert output is not None

        output.write(dump)
        output.flush()
        return _ExecutionResult()

    return execute


def test_analyze_profile_log_filters_by_node_name(tmp_path):
    dump = "warning\n\n" + "".join([
        _event_line("ReadData", "{node_name=selected, node_id=1}"),
        _event_line("WriteData", "{node_name=other, node_id=2}"),
        _event_line("ReadData", "{node_name=selected, node_id=3}"),
    ])
    execute = _execute_writer(dump)

    with mock.patch.object(
        profile_log.common,
        "output_path",
        return_value=str(tmp_path),
    ), mock.patch.object(
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
    assert list(tmp_path.iterdir()) == []


def test_iter_profile_log_events_handles_arbitrary_values(tmp_path):
    dump = b"warning\n" + _event_line(
        "ReadData",
        "client_id=foo=bar",
        "name=invalid",
    ).encode("utf-8").replace(b"name=invalid", b"name=invalid\xff")
    execute = _execute_writer(dump)

    with mock.patch.object(
        profile_log.common,
        "output_path",
        return_value=str(tmp_path),
    ), mock.patch.object(
        profile_log.common,
        "execute",
        side_effect=execute,
    ):
        events = list(profile_log.iter_profile_log_events(
            "/test/filestore-profile-tool",
            "/test/profile.log",
            "nfs_test",
        ))

    assert events == [(
        "ReadData",
        {
            "client_id": "foo=bar",
            "name": "invalid\ufffd",
        },
    )]


def test_dumpevents_output_is_preserved_on_failure(tmp_path):
    dump = _event_line("ReadData", "{node_id=1}")

    def execute(_command, **kwargs):
        kwargs["stdout"].write(dump.encode("utf-8"))
        raise RuntimeError("dumpevents failed")

    with mock.patch.object(
        profile_log.common,
        "output_path",
        return_value=str(tmp_path),
    ), mock.patch.object(
        profile_log.common,
        "execute",
        side_effect=execute,
    ), pytest.raises(RuntimeError, match="dumpevents failed"):
        profile_log.analyze_profile_log(
            "/test/filestore-profile-tool",
            "/test/profile.log",
            "nfs_test",
        )

    output_files = list(tmp_path.iterdir())
    assert len(output_files) == 1
    assert output_files[0].read_text() == dump
