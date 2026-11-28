"""Isolated NBS/YDB tracing acceptance harness; never connects to production."""
import concurrent.futures
import datetime
import hashlib
import html
import http.server
import json
from pathlib import Path
import struct
import threading
import time

import grpc
from google.protobuf.text_format import MessageToString
import yatest.common as common

from cloud.blockstore.config.diagnostics_pb2 import TDiagnosticsConfig
from cloud.blockstore.public.api.grpc.service_pb2_grpc import TBlockStoreServiceStub
from cloud.blockstore.public.api.protos import io_pb2, mount_pb2, volume_pb2
from cloud.blockstore.tests.python.lib.config import storage_config_with_default_limits
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import wait_for_nbs_server
from contrib.ydb.core.protos.config_pb2 import TTracingConfig
from contrib.ydb.core.protos import msgbus_pb2
from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True))


def sha(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def trace_id(request_id):
    return struct.pack("=QQ", request_id, 0)


def attribute_value(value):
    kind = value.WhichOneof("value")
    if kind == "array_value":
        return [attribute_value(item) for item in value.array_value.values]
    if kind == "kvlist_value":
        return {item.key: attribute_value(item.value)
                for item in value.kvlist_value.values}
    if kind == "bytes_value":
        return value.bytes_value.hex()
    return getattr(value, kind) if kind else None


def attributes(span):
    return {item.key: attribute_value(item.value) for item in span.attributes}


class Collector(trace_service_pb2_grpc.TraceServiceServicer):
    def __init__(self, output):
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=False)
        self.lock = threading.RLock()
        self.spans = {}
        self.receipts = []
        self.duplicates = []
        self.server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=4))
        trace_service_pb2_grpc.add_TraceServiceServicer_to_server(self, self.server)
        self.port = self.server.add_insecure_port("127.0.0.1:0")
        assert self.port
        self.server.start()
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                value = self.path.removeprefix("/trace/")
                try:
                    selected = bytes.fromhex(value)
                    assert len(selected) == 16
                except (ValueError, AssertionError):
                    self.send_error(400)
                    return
                with owner.lock:
                    rows = [
                        (key, span) for key, (_, span) in owner.spans.items()
                        if key[0] == selected
                    ]
                body = "<!doctype html><title>Captured trace</title><h1>" + value + "</h1>"
                for key, span in rows:
                    body += "<p>" + html.escape(span.name) + " " + key[1].hex() + "</p>"
                    for link in span.links:
                        body += '<a href="/trace/' + link.trace_id.hex() + '">Linked trace</a>'
                self.send_response(200 if rows else 404)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(body.encode())

            def log_message(self, *_):
                pass

        self.http = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.http_thread = threading.Thread(target=self.http.serve_forever, daemon=True)
        self.http_thread.start()

    @property
    def viewer_template(self):
        return "http://127.0.0.1:%s/trace/{trace_id}" % self.http.server_port

    def Export(self, request, context):
        raw = request.SerializeToString()
        with self.lock:
            path = self.output / ("%06d.otlp" % len(self.receipts))
            path.write_bytes(raw)
            count = 0
            for resource in request.resource_spans:
                service = next(
                    (a.value.string_value for a in resource.resource.attributes
                     if a.key == "service.name"), "")
                for scope in resource.scope_spans:
                    for span in scope.spans:
                        count += 1
                        key = (span.trace_id, span.span_id)
                        old = self.spans.get(key)
                        if old and old[1].SerializeToString() != span.SerializeToString():
                            self.duplicates.append([key[0].hex(), key[1].hex()])
                        if len(self.spans) < 50000:
                            copy = type(span)()
                            copy.CopyFrom(span)
                            self.spans[key] = (service, copy)
            self.receipts.append(dict(
                file=path.name, sha256=hashlib.sha256(raw).hexdigest(),
                received_at=utc(), bytes=len(raw), spans=count))
        return trace_service_pb2.ExportTraceServiceResponse()

    def snapshot(self):
        with self.lock:
            return dict(self.spans)

    def close(self):
        self.http.shutdown()
        self.http.server_close()
        self.http_thread.join(timeout=5)
        self.server.stop(5).wait(10)
        with self.lock:
            write_json(self.output / "receipts.json", self.receipts)
            write_json(self.output / "conflicting_ids.json", self.duplicates)
            write_json(self.output / "spans.json", [
                dict(service=service, trace_id=key[0].hex(), span_id=key[1].hex(),
                     parent_span_id=span.parent_span_id.hex(),
                     name=span.name, start=span.start_time_unix_nano,
                     end=span.end_time_unix_nano, attributes=attributes(span),
                     status=span.status.code,
                     links=[dict(trace_id=x.trace_id.hex(), span_id=x.span_id.hex())
                            for x in span.links])
                for key, (service, span) in self.spans.items()
            ])


class Environment:
    def __init__(self, output, nbs_binary=None, sampling=1, viewer=True, throttling=True):
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=False)
        self.collector = Collector(self.output / "collector")
        self.cluster = None
        self.nbs = None
        self.channel = None
        self.sequence = 731100000
        self.sequence_lock = threading.Lock()
        self.requests = []
        self.nbs_binary = nbs_binary or common.binary_path("cloud/blockstore/apps/server/nbsd")
        self.ydb_binary = common.binary_path("contrib/ydb/apps/ydbd/ydbd")
        try:
            self._start(sampling, viewer, throttling)
        except BaseException:
            self.close()
            raise

    def _start(self, sampling, viewer, throttling):
        pools = [
            dict(name="dynamic_storage_pool:1", kind="rot", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)]
        collector_url = "grpc://127.0.0.1:%s" % self.collector.port
        config = KikimrConfigGenerator(
            erasure=None, binary_paths=[self.ydb_binary],
            use_in_memory_pdisks=True, dynamic_storage_pools=pools)
        config.yaml_config["tracing_config"] = {
            "backend": {"opentelemetry": {
                "collector_url": collector_url, "service_name": "ydb-tracing-test"}},
            "external_throttling": [{"level": 15, "max_traces_per_minute": 1000000,
                                     "max_traces_burst": 1000000}],
            "uploader": {"max_exported_spans_per_second": 1000000,
                         "max_batch_accumulation_milliseconds": 100}}
        self.cluster = kikimr_cluster_factory(configurator=config)
        self.cluster.start()
        tracing = TTracingConfig()
        tracing.Backend.Opentelemetry.CollectorUrl = collector_url
        tracing.Backend.Opentelemetry.ServiceName = "ydb-tracing-test"
        tracing.Uploader.MaxBatchAccumulationMilliseconds = 100
        tracing.Uploader.MaxExportedSpansPerSecond = 1000000
        rule = tracing.ExternalThrottling.add()
        rule.Level = 15
        rule.MaxTracesPerMinute = 1000000
        rule.MaxTracesBurst = 1000000
        request = msgbus_pb2.TConsoleRequest()
        action = request.ConfigureRequest.Actions.add()
        action.AddConfigItem.ConfigItem.Config.TracingConfig.CopyFrom(tracing)
        action.AddConfigItem.EnableAutoSplit = True
        response = self.cluster.client.invoke(request, "ConsoleRequest")
        assert response.Status.Code == StatusIds.SUCCESS, response

        diagnostics = TDiagnosticsConfig()
        diagnostics.SamplingRate = sampling
        diagnostics.SlowRequestSamplingRate = 0
        diagnostics.PassTraceIdToBlobstorage = True
        otel = diagnostics.OpentelemetryTraceConfig
        otel.ServiceName = "nbs-tracing-test"
        otel.ClientConfig.Address = "127.0.0.1:%s" % self.collector.port
        otel.ClientConfig.Insecure = True
        otel.ClientConfig.RequestTimeout = 10000
        if viewer:
            otel.TraceViewerUrlTemplate = self.collector.viewer_template
        storage = storage_config_with_default_limits()
        storage.ThrottlingEnabled = throttling
        storage.ThrottlingEnabledSSD = throttling
        storage.FreshChannelWriteRequestsEnabled = True
        storage.FreshChannelZeroRequestsEnabled = True
        storage.MaxWriteBlobErrorsBeforeSuicide = 100
        self.nbs = LocalNbs(
            next(iter(self.cluster.nodes.values())).port, config.domains_txt,
            storage_config_patches=[storage], diagnostics_config_patch=diagnostics,
            dynamic_storage_pools=pools, nbs_binary_path=self.nbs_binary,
            kikimr_binary_path=self.ydb_binary, load_configs_from_cms=False)
        self.nbs.setup_cms(self.cluster.client)
        self.nbs.start()
        wait_for_nbs_server(self.nbs.nbs_port)
        self.channel = grpc.insecure_channel(
            "127.0.0.1:%s" % self.nbs.nbs_port,
            options=[("grpc.max_receive_message_length", 16 * 1024 * 1024)])
        grpc.channel_ready_future(self.channel).result(timeout=30)
        self.client = TBlockStoreServiceStub(self.channel)
        (self.output / "diagnostics.txt").write_text(MessageToString(diagnostics))
        (self.output / "storage.txt").write_text(MessageToString(storage))
        (self.output / "ydb-tracing.txt").write_text(MessageToString(tracing))
        write_json(self.output / "binaries.json", {
            "nbs": {"path": self.nbs_binary, "sha256": sha(self.nbs_binary)},
            "ydb": {"path": self.ydb_binary, "sha256": sha(self.ydb_binary)}})

    def header(self, request, request_id=None):
        if request_id is None:
            with self.sequence_lock:
                self.sequence += 1
                request_id = self.sequence
        request.Headers.RequestId = request_id
        request.Headers.ClientId = "nbs-7311"
        request.Headers.Timestamp = time.time_ns() // 1000
        return request

    def call(self, method, request, request_id=None, error=False):
        self.header(request, request_id)
        start = utc()
        response = getattr(self.client, method)(request, timeout=60)
        failed = bool(response.Error.Code & 0x80000000)
        self.requests.append(dict(method=method, disk=request.DiskId,
                                  request_id=request.Headers.RequestId,
                                  start=start, end=utc(), error=response.Error.Code))
        assert failed == error, response
        return response

    def volume(self, media, disk):
        request = volume_pb2.TCreateVolumeRequest(
            DiskId=disk, BlocksCount=65536, BlockSize=4096,
            StorageMediaKind=media, TabletVersion=1, PartitionsCount=1)
        self.call("CreateVolume", request)
        response = self.call("MountVolume", mount_pb2.TMountVolumeRequest(
            DiskId=disk, InstanceId="tracing-test"))
        session = response.SessionId
        assert session
        request = io_pb2.TWriteBlocksRequest(DiskId=disk, SessionId=session)
        request.Blocks.Buffers.append(b"p" * (1024 * 1024))
        self.call("WriteBlocks", request)
        return session

    def operation(self, operation, disk, session, index=0, blocks=1):
        cls = getattr(io_pb2, "T%sRequest" % operation)
        request = cls(DiskId=disk, SessionId=session, StartIndex=index)
        if operation == "WriteBlocks":
            request.Blocks.Buffers.append(b"w" * (4096 * blocks))
        else:
            request.BlocksCount = blocks
        return request

    def close(self):
        errors = []
        if self.channel:
            self.channel.close()
        for service in (self.nbs, self.cluster):
            if service:
                try:
                    service.stop()
                except Exception as error:
                    errors.append(repr(error))
        try:
            self.collector.close()
        except Exception as error:
            errors.append("collector: " + repr(error))
        write_json(self.output / "requests.json", self.requests)
        write_json(self.output / "cleanup.json", errors)
        if errors:
            raise RuntimeError("Isolated services failed to stop: " + repr(errors))


def validate_trace(captured, request_id, operation, require_ydb=True, error=False):
    tid = trace_id(request_id)
    local = {key: span for key, (service, span) in captured.items() if key[0] == tid}
    names = {span.name for span in local.values()}
    stages = ("Server", "Service") if error else ("Server", "Service", "Throttler")
    for stage in stages:
        assert stage + "." + operation in names, (request_id, stage, sorted(names))
    if error:
        assert any(span.name == "Server." + operation and span.status.code == 2
                   for span in local.values()), "Missing error on the completed server span"
    for span in local.values():
        assert span.span_id and span.span_id != b"\0" * 8
        assert span.start_time_unix_nano <= span.end_time_unix_nano
    bridges = {key for key, span in local.items() if span.name.startswith("BlobStorage.")}
    if not require_ydb:
        assert not bridges, "A rejected session must not be attributed a BlobStorage request"
        return dict(trace_id=tid.hex(), names=sorted(names), bridges=[], reached=[])
    assert bridges, (request_id, sorted(names))
    children = {}
    for key, (_, span) in captured.items():
        children.setdefault((key[0], span.parent_span_id), []).append(key)
    reached = set(bridges)
    queue = list(bridges)
    links = []
    while queue:
        key = queue.pop()
        span = captured[key][1]
        destinations = children.get(key, [])
        for link in span.links:
            target = (link.trace_id, link.span_id)
            assert target in captured, ("missing linked YDB span", target)
            destinations = destinations + [target]
            links.append(dict(source=[x.hex() for x in key],
                              target=[x.hex() for x in target]))
        for target in destinations:
            if target not in reached:
                reached.add(target)
                queue.append(target)
    related = [captured[key][1] for key in reached]
    assert links, (request_id, "No causal DSProxy link")
    assert any("GroupId" in attributes(span) for span in related), "Missing group"
    assert any(span.name.startswith("VDisk.") for span in related), "Missing VDisk"
    assert all(children.get(key) for key in bridges), "Wire parent not exported"
    return dict(trace_id=tid.hex(), links=links, names=sorted({x.name for x in related}),
                reached=sorted([[a.hex(), b.hex()] for a, b in reached]),
                bridges=sorted([[a.hex(), b.hex()] for a, b in bridges]))


def wait_for_traces(env, cases, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        captured = env.collector.snapshot()
        try:
            result = [validate_trace(
                captured, case["request_id"], case["operation"],
                require_ydb=not case.get("error"), error=bool(case.get("error")))
                for case in cases]
            assert not env.collector.duplicates, env.collector.duplicates
            return result
        except AssertionError as error:
            last = error
            time.sleep(1)
    raise AssertionError("Traces did not satisfy acceptance oracle: %s" % last)
