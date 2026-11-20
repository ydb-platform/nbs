import concurrent.futures
from pathlib import Path
import urllib.parse
import urllib.request

import pytest
import yatest.common as common

from cloud.storage.core.protos import media_pb2
from cloud.blockstore.tests.tracing.tracing_harness import Environment, trace_id, wait_for_traces, write_json
from cloud.blockstore.tests.tracing.tracing_perf import run_comparison


def test_collector_attribute_values():
    import json
    from opentelemetry.proto.trace.v1.trace_pb2 import Span
    from cloud.blockstore.tests.tracing.tracing_harness import attributes

    span = Span()
    nested = span.attributes.add(key="nested").value.array_value
    nested.values.add().string_value = "value"
    nested.values.add().int_value = 7
    item = nested.values.add().kvlist_value.values.add(key="raw")
    item.value.bytes_value = b"\x00\xff"
    span.attributes.add(key="flag").value.bool_value = True
    span.attributes.add(key="unset")
    encoded = json.dumps(attributes(span))
    assert json.loads(encoded) == {
        "nested": ["value", 7, {"raw": "00ff"}],
        "flag": True, "unset": None}


def test_data_path_traces():
    output = Path(common.output_path("data-path-traces"))
    env = Environment(output)
    cases = []
    try:
        for media, name in ((media_pb2.STORAGE_MEDIA_SSD, "ssd"),
                            (media_pb2.STORAGE_MEDIA_HDD, "hdd")):
            disk = "trace-" + name
            session = env.volume(media, disk)
            for index, operation in enumerate(("ReadBlocks", "WriteBlocks", "ZeroBlocks")):
                request_id = 731100 + len(cases)
                request = env.operation(operation, disk, session, index=index)
                result = env.call(operation, request, request_id)
                if operation == "ReadBlocks":
                    assert b"".join(result.Blocks.Buffers) == b"p" * 4096
                cases.append(dict(media=name, disk=disk, operation=operation,
                                  request_id=request_id))
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                futures = []
                for index, operation in enumerate(("WriteBlocks", "ZeroBlocks")):
                    request_id = 731100 + len(cases)
                    cases.append(dict(media=name, disk=disk, operation=operation,
                                      request_id=request_id, concurrent=True))
                    request = env.operation(operation, disk, session, index=10 + index)
                    futures.append(pool.submit(env.call, operation, request, request_id))
                for future in futures:
                    future.result()
            # Repeated calls with the same NBS request id need distinct attempts.
            request_id = 731100 + len(cases)
            for _ in range(2):
                env.call("WriteBlocks",
                         env.operation("WriteBlocks", disk, session, index=20),
                         request_id)
            cases.append(dict(media=name, disk=disk, operation="WriteBlocks",
                              request_id=request_id, attempts=2))

        # Session rejection gives each API operation a deterministic error trace.
        # This does not replace the actor-level BlobStorage error scenarios.
        for media, name in ((media_pb2.STORAGE_MEDIA_SSD, "ssd"),
                            (media_pb2.STORAGE_MEDIA_HDD, "hdd")):
            disk = "trace-" + name
            for operation in ("ReadBlocks", "WriteBlocks", "ZeroBlocks"):
                request_id = 731100 + len(cases)
                env.call(operation, env.operation(
                    operation, disk, "invalid-session"), request_id, error=True)
                cases.append(dict(media=name, disk=disk, operation=operation,
                                  request_id=request_id, error=True,
                                  fault="invalid-session"))
        evidence = wait_for_traces(env, cases)
        graph_owners = {}
        for case, result in zip(cases, evidence):
            if not case.get("error"):
                assert len(result["bridges"]) >= case.get("attempts", 1)
            for reached in result["reached"]:
                key = tuple(reached)
                owner = graph_owners.setdefault(key, case["request_id"])
                assert owner == case["request_id"], ("cross-request correlation", key)
        # Include the collector drain interval in the monitoring time range.
        from cloud.blockstore.tests.tracing.tracing_harness import utc
        query = urllib.parse.urlencode({
            "diskId": cases[0]["disk"],
            "from": env.requests[0]["start"], "to": utc()})
        monitor_url = "http://127.0.0.1:%s/tracelogs/random?%s" % (env.nbs.mon_port, query)
        page = urllib.request.urlopen(monitor_url, timeout=10).read().decode()
        (output / "monitoring.html").write_text(page)
        wanted = env.collector.viewer_template.replace(
            "{trace_id}", trace_id(cases[0]["request_id"]).hex())
        assert wanted in page, "Monitoring page did not link the known request"
        trace_page = urllib.request.urlopen(wanted, timeout=10).read().decode()
        assert "Server.ReadBlocks" in trace_page
        (output / "trace.html").write_text(trace_page)
        write_json(output / "acceptance.json", dict(
            cases=cases, correlation=evidence,
            monitoring_url=monitor_url, trace_url=wanted,
            checks={"matrix": True, "concurrency": True, "repeated_request_id": True,
                    "monitoring_http": True, "api_errors": True},
            manual_ui_seconds=None, manual_ui_status="not_measured"))
    finally:
        env.close()


def test_tracing_performance():
    # The paired benchmark is an explicit acceptance scenario, not a CI default.
    if common.get_param("tracing_performance") != "true":
        pytest.skip("Run the paired benchmark with tracing_performance=true and pinned release binaries")
    # Missing inputs in the selected benchmark remain errors.
    baseline = common.get_param("tracing_baseline")
    assert baseline and Path(baseline).is_file(), "tracing_baseline must be an existing absolute binary path"
    assert Path(baseline).is_absolute()
    run_comparison(Path(common.output_path("tracing-performance")), baseline)
