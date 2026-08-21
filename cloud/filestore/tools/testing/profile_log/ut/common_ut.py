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
    def execute(_command, **kwargs):
        output = kwargs.get("stdout")
        assert output is not None

        output.write(dump.encode("utf-8"))
        output.flush()
        return _ExecutionResult()

    return execute


def test_analyze_profile_log_filters_by_node_name():
    dump = "".join([
        _event_line("ReadData", "{node_name=selected, node_id=1}"),
        _event_line("WriteData", "{node_name=other, node_id=2}"),
        _event_line("ReadData", "{node_name=selected, node_id=3}"),
    ])
    execute = _execute_writer(dump)

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
