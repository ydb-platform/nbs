import grpc
import time
import threading

import contrib.ydb.core.protos.grpc_pb2_grpc as grpc_server

from concurrent import futures

from contrib.ydb.tests.library.common.yatest_common import PortManager

from contrib.ydb.public.api.protos import ydb_discovery_pb2
from contrib.ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc

from cloud.blockstore.config.storage_pb2 import TStorageServiceConfig

from cloud.blockstore.tests.python.lib.config import (
    NbsConfigurator,
)

from cloud.blockstore.tests.python.lib.client import NbsClient
from cloud.blockstore.tests.python.lib.daemon import (
    start_ydb,
    start_nbs,
)

from contrib.ydb.core.protos import msgbus_pb2 as msgbus
from contrib.ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from google.protobuf.text_format import MessageToString
from google.protobuf.message import Message

CFG_PREFIX = "Cloud.NBS."


class ProxyInterceptor(grpc.ServerInterceptor):
    def __init__(self, proxy_target, cred):
        self.proxy_target = proxy_target
        self._channel = grpc.secure_channel(self.proxy_target, cred)
        self._stub = grpc_server.TGRpcServerStub(self._channel)
        self._implemented_methods = set()
        super().__init__()

    def register_method(self, method_name):
        self._implemented_methods.add(method_name)

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method

        if method in self._implemented_methods:
            return continuation(handler_call_details)

        def proxy_handler(request, context):
            try:
                if isinstance(request, Message):
                    request_bytes = request.SerializeToString()
                else:
                    request_bytes = request

                response_bytes = self._channel.unary_unary(
                    method,
                    request_serializer=lambda x: x,
                    response_deserializer=lambda x: x,
                )(request_bytes)

                return response_bytes
            except grpc.RpcError as e:
                context.set_code(e.code())
                context.set_details(e.details())
                return None
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Proxy error: {str(e)}")
                return None

        return grpc.unary_unary_rpc_method_handler(
            proxy_handler,
            request_deserializer=lambda x: x,
            response_serializer=lambda x: x,
        )

# This server simulates a hanging connection. It proxies all requests to the YDB server,
# except for NodeRegistration requests which are handled specially:
# - For the first max_node_reg_count NodeRegistration attempts, the server receives
# the request but deliberately does not respond
# - After these initial attempts, all subsequent NodeRegistration requests
# are proxied normally to the YDB server.
# Names of methods ListEndpoints, WhoAmI, and NodeRegistration are written in camel case.
# This naming style is used because these methods override the corresponding methods of the
# interface ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer.


class DiscoveryServiceServicer(ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer):

    def __init__(
        self,
        max_node_reg_count=1,
        stop_event=threading.Event(),
        ydb=None,
    ):
        self.node_registration_count = 0
        self.max_node_reg_count = max_node_reg_count
        self.secondary_host = "localhost"
        self.secondary_port = list(ydb.nodes.values())[0].grpc_ssl_port
        self.lock = threading.Lock()
        self.stop_event = stop_event

        self.secondary_channel = None
        self.ydb_stub = None
        self.ydb = ydb

    def _get_ydb_stub(self):
        if self.ydb_stub is None:
            with open(self.ydb.config.grpc_tls_ca_path, "rb") as f:
                ca_cert = f.read()
            with open(self.ydb.config.grpc_tls_cert_path, "rb") as f:
                client_cert = f.read()
            with open(self.ydb.config.grpc_tls_key_path, "rb") as f:
                client_key = f.read()

            creds = grpc.ssl_channel_credentials(
                root_certificates=ca_cert,
                private_key=client_key,
                certificate_chain=client_cert,
            )

            self.secondary_channel = grpc.secure_channel(
                f"{self.secondary_host}:{self.secondary_port}",
                creds,
                options=(("grpc.ssl_target_name_override", "localhost"),),
            )

            self.ydb_stub = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(
                self.secondary_channel
            )

        return self.ydb_stub

    def ListEndpoints(self, request, context):
        try:
            ydb_stub = self._get_ydb_stub()
            secondary_response = ydb_stub.ListEndpoints(request)

            return secondary_response
        except Exception:
            return ydb_discovery_pb2.ListEndpointsResponse()

    def WhoAmI(self, request, context):
        try:
            ydb_stub = self._get_ydb_stub()
            secondary_response = ydb_stub.WhoAmI(request)

            return secondary_response
        except Exception:
            return ydb_discovery_pb2.WhoAmIResponse()

    def NodeRegistration(self, request, context):
        with self.lock:
            self.node_registration_count += 1
            current_count = self.node_registration_count

        if current_count <= self.max_node_reg_count:
            try:
                while not self.stop_event.is_set():
                    time.sleep(0.5)
            except Exception:
                pass
        else:
            try:
                ydb_stub = self._get_ydb_stub()
                secondary_response = ydb_stub.NodeRegistration(request)

                return secondary_response

            except grpc.RpcError as e:
                context.set_code(e.code())
                context.set_details(
                    f"Error forwarding to secondary server: {e.details()}"
                )
                return ydb_discovery_pb2.NodeRegistrationResponse()
            except Exception as e:
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Internal error: {str(e)}")
                return ydb_discovery_pb2.NodeRegistrationResponse()


class ServerThread(threading.Thread):
    def __init__(self, server):
        threading.Thread.__init__(self)
        self.server = server
        self.daemon = True

    def run(self):
        self.server.start()


def serve(not_responding_server_port, ydb):
    with open(ydb.config.grpc_tls_key_path, "rb") as f:
        private_key = f.read()
    with open(ydb.config.grpc_tls_cert_path, "rb") as f:
        certificate_chain = f.read()
    with open(ydb.config.grpc_tls_ca_path, "rb") as f:
        root_certificates = f.read()

    interceptor = ProxyInterceptor(
        f"localhost:{list(ydb.nodes.values())[0].grpc_ssl_port}",
        grpc.ssl_channel_credentials(
            root_certificates=root_certificates,
            private_key=private_key,
            certificate_chain=certificate_chain,
        ),
    )

    interceptor.register_method("/Ydb.Discovery.V1.DiscoveryService/ListEndpoints")
    interceptor.register_method("/Ydb.Discovery.V1.DiscoveryService/WhoAmI")
    interceptor.register_method("/Ydb.Discovery.V1.DiscoveryService/NodeRegistration")

    server_credentials = grpc.ssl_server_credentials(
        [(private_key, certificate_chain)],
        root_certificates=root_certificates,
        require_client_auth=False,
    )

    # This server simulates a hanging connection. When a client sends a request,
    # the server receives it but deliberately does not respond.
    # Default settings would trigger keepalive timeout errors,
    # so we configure additional options to prevent these errors.

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.keepalive_time_ms", 1000),
            ("grpc.keepalive_timeout_ms", 1000),
            ("grpc.max_connection_idle_ms", 24 * 3600 * 1000),
            ("grpc.max_connection_age_ms", 24 * 3600 * 1000),
            ("grpc.max_connection_age_grace_ms", 24 * 3600 * 1000),
            (
                "grpc.http2.min_time_between_pings_ms",
                0,
            ),
            (
                "grpc.keepalive_permit_without_calls",
                1,
            ),
        ],
        interceptors=(interceptor,),
    )

    stop_event = threading.Event()

    ydb_discovery_v1_pb2_grpc.add_DiscoveryServiceServicer_to_server(
        DiscoveryServiceServicer(
            max_node_reg_count=2,
            stop_event=stop_event,
            ydb=ydb,
        ),
        server,
    )

    server.add_secure_port(
        f"localhost:{not_responding_server_port}", server_credentials
    )

    server_thread = ServerThread(server)
    server_thread.start()

    time.sleep(1)

    return server, server_thread, stop_event


def update_cms_config(ydb_client, name, config, node_type):
    req = msgbus.TConsoleRequest()
    action = req.ConfigureRequest.Actions.add()

    custom_cfg = action.AddConfigItem.ConfigItem.Config.NamedConfigs.add()
    custom_cfg.Name = CFG_PREFIX + name
    custom_cfg.Config = MessageToString(config, as_one_line=True).encode()

    s = action.AddConfigItem.ConfigItem.UsageScope

    s.TenantAndNodeTypeFilter.Tenant = "/Root/nbs"
    s.TenantAndNodeTypeFilter.NodeType = node_type

    action.AddConfigItem.ConfigItem.MergeStrategy = 1  # OVERWRITE

    response = ydb_client.invoke(req, "ConsoleRequest")
    assert response.Status.Code == StatusIds.SUCCESS


def setup_cms_configs(ydb_client):
    # blockstore-server control
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = "nbs_control"
    storage.SchemeShardDir = "/Root/nbs"

    update_cms_config(ydb_client, "StorageServiceConfig", storage, "nbs_control")

    # blockstore-server
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = "nbs"
    storage.SchemeShardDir = "/Root/nbs"

    update_cms_config(ydb_client, "StorageServiceConfig", storage, "nbs")

    # disk-agent
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = "disk-agent"
    storage.SchemeShardDir = "/Root/nbs"

    update_cms_config(ydb_client, "StorageServiceConfig", storage, "disk-agent")

    # global
    storage = TStorageServiceConfig()
    storage.DisableLocalService = False
    storage.ManuallyPreemptedVolumesFile = ""
    storage.SchemeShardDir = "/Root/nbs"

    update_cms_config(ydb_client, "StorageServiceConfig", storage, "")


def prepare(
    ydb,
    node_type,
):
    nbs_configurator = NbsConfigurator(
        ydb,
        ssl_registration=True,
    )
    nbs_configurator.generate_default_nbs_configs()

    nbs_configurator.files["storage"].NodeRegistrationRootCertsFile = (
        ydb.config.grpc_tls_ca_path
    )
    nbs_configurator.files["storage"].NodeRegistrationCert.CertFile = (
        ydb.config.grpc_tls_cert_path
    )
    nbs_configurator.files["storage"].NodeRegistrationCert.CertPrivateKeyFile = (
        ydb.config.grpc_tls_key_path
    )
    nbs_configurator.files["storage"].NodeType = node_type
    nbs_configurator.files["storage"].DisableLocalService = False
    nbs_configurator.files["storage"].DynamicNodeRegistrationTimeout = 1000  # 1 second

    return nbs_configurator


def setup_and_run_test_for_server(node_type):
    ydb = start_ydb(grpc_ssl_enable=True)
    setup_cms_configs(ydb.client)

    not_responding_server_port = PortManager().get_port()

    server, server_thread, stop_event = serve(not_responding_server_port, ydb)

    nbs = start_nbs(
        prepare(ydb, node_type),
        ydb_ssl_port=not_responding_server_port,
    )

    client = NbsClient(nbs.port)

    r = client.get_storage_service_config().get("ManuallyPreemptedVolumesFile")
    assert r == node_type

    nbs.kill()

    stop_event.set()
    server.stop(0)
    server_thread.join(timeout=1)

    return True


def test_server_registration_with_timeout():
    assert setup_and_run_test_for_server("nbs")
