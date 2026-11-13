"""Manual TLS certificate updates with the "updatecertificates" action.

Periodic certificate checks are disabled, so filestore-server and
filestore-vhost apply new certificate files only when the action asks for it.
"""

import collections
import json
import os
import shutil
import socket
import ssl
import tempfile

import pytest
import requests

import yatest.common as yatest_common

from cloud.filestore.config.server_pb2 import TNullServiceConfig, \
    TServerAppConfig, TServerConfig
from cloud.filestore.config.vhost_pb2 import TServiceEndpoint, \
    TVhostAppConfig, TVhostServiceConfig
from cloud.filestore.public.sdk.python.client import ClientCredentials, \
    ClientError, CreateClient, CreateEndpointClient, EResult
from cloud.filestore.tests.python.lib.client import FilestoreCliClient
from cloud.filestore.tests.python.lib.daemon_config import \
    FilestoreServerConfigGenerator, FilestoreVhostConfigGenerator
from cloud.filestore.tests.python.lib.server import FilestoreServer, \
    wait_for_filestore_server
from cloud.filestore.tests.python.lib.vhost import FilestoreVhost, \
    wait_for_filestore_vhost
from cloud.storage.core.protos.endpoints_pb2 import EEndpointStorageType


CERTS_DIR = "cloud/filestore/tests/certs"


def source_file(name):
    return yatest_common.source_path(os.path.join(CERTS_DIR, name))


def read_certificate(name):
    with open(source_file(name)) as f:
        return ssl.PEM_cert_to_DER_cert(f.read())


class CertificateFiles:

    def __init__(self):
        path = tempfile.mkdtemp(dir=yatest_common.output_path())
        self.cert_file = os.path.join(path, "server.crt")
        self.key_file = os.path.join(path, "server.key")
        self.root_certs_file = os.path.join(path, "root.crt")

    def install(self, cert, key, root_certs):
        for name, path in [
                (cert, self.cert_file),
                (key, self.key_file),
                (root_certs, self.root_certs_file)]:
            # Written aside and renamed over the old file, as an update
            # should do it: the server never reads a half-written file.
            tmp_path = path + ".tmp"
            shutil.copyfile(source_file(name), tmp_path)
            os.replace(tmp_path, path)


def get_server_certificate(port, name):
    """Returns the certificate the server presents in a new TLS handshake,
    which must be certificate |name|."""

    context = ssl.create_default_context(cafile=source_file(name))
    context.set_alpn_protocols(["h2"])

    with socket.create_connection(("localhost", port)) as sock:
        with context.wrap_socket(sock, server_hostname="localhost") as tls:
            assert tls.getpeercert(binary_form=True) == read_certificate(name)
            return tls.getpeercert()


def get_expire_ts(cert):
    return ssl.cert_time_to_seconds(cert["notAfter"])


CertificateMetrics = collections.namedtuple(
    "CertificateMetrics",
    ["cert_version", "root_version", "expire_ts"])


def get_certificate_metrics(daemon, certs):
    """Returns what the metrics of |daemon| report for the files in use: the
    versions of the certificate and the root certificates and the expiration
    time of the certificate."""

    r = requests.get(
        f"http://localhost:{daemon.mon_port}"
        "/counters/counters=filestore/json",
        timeout=10)
    r.raise_for_status()

    metrics = {}
    for sensor in r.json()["sensors"]:
        labels = sensor["labels"]
        if labels.get("component") == "certificates":
            metrics[(labels.get("path"), labels.get("sensor"))] = \
                sensor["value"]

    def get(path, name):
        value = metrics.get((path, name))
        assert value is not None, f"No {name} metric for {path}"
        return value

    return CertificateMetrics(
        cert_version=get(certs.cert_file, "Fingerprint"),
        root_version=get(certs.root_certs_file, "Fingerprint"),
        expire_ts=get(certs.cert_file, "ExpireTs"))


def new_credentials():
    """Credentials of a client that trusts only the new certificate and
    presents it."""

    return ClientCredentials(
        root_certs_file=source_file("server_new.crt"),
        cert_file=source_file("server_new.crt"),
        cert_private_key_file=source_file("server_new.key"))


def old_certificates():
    files = CertificateFiles()
    files.install(
        cert="server.crt",
        key="server.key",
        root_certs="server.crt")
    return files


@pytest.fixture
def certs():
    return old_certificates()


@pytest.fixture
def client_certs():
    return old_certificates()


@pytest.fixture
def filestore(certs):
    config = TServerAppConfig()
    config.NullServiceConfig.CopyFrom(TNullServiceConfig())
    config.ServerConfig.CopyFrom(TServerConfig())
    config.ServerConfig.RootCertsFile = certs.root_certs_file
    cert = config.ServerConfig.Certs.add()
    cert.CertFile = certs.cert_file
    cert.CertPrivateKeyFile = certs.key_file
    config.ServerConfig.RefreshCertsPeriod = 0

    configurator = FilestoreServerConfigGenerator(
        binary_path=yatest_common.binary_path(
            "cloud/filestore/apps/server/filestore-server"),
        app_config=config,
        service_type="null",
        verbose=True,
        secure=True)
    configurator.generate_configs()

    server = FilestoreServer(configurator=configurator)
    server.start()
    try:
        wait_for_filestore_server(server, configurator.port)
        yield configurator
    finally:
        server.stop()


@pytest.fixture
def vhost(certs, client_certs):
    """filestore-vhost with |certs| for its own secure port and with
    |client_certs| for the filestore-server it forwards filesystem requests to.
    There is no such server: the client certificates are read, but nothing
    connects with them."""

    endpoint = TServiceEndpoint()
    endpoint.ClientConfig.Host = "localhost"
    endpoint.ClientConfig.SecurePort = 1
    endpoint.ClientConfig.RootCertsFile = client_certs.root_certs_file
    endpoint.ClientConfig.CertFile = client_certs.cert_file
    endpoint.ClientConfig.CertPrivateKeyFile = client_certs.key_file

    config = TVhostAppConfig()
    config.VhostServiceConfig.CopyFrom(TVhostServiceConfig())
    config.VhostServiceConfig.ServiceEndpoints.append(endpoint)
    config.VhostServiceConfig.EndpointStorageType = \
        EEndpointStorageType.ENDPOINT_STORAGE_FILE
    config.VhostServiceConfig.EndpointStorageDir = \
        tempfile.mkdtemp(dir=yatest_common.output_path())
    config.ServerConfig.CopyFrom(TServerConfig())
    config.ServerConfig.RootCertsFile = certs.root_certs_file
    cert = config.ServerConfig.Certs.add()
    cert.CertFile = certs.cert_file
    cert.CertPrivateKeyFile = certs.key_file
    config.ServerConfig.RefreshCertsPeriod = 0

    configurator = FilestoreVhostConfigGenerator(
        binary_path=yatest_common.binary_path(
            "cloud/filestore/apps/vhost/filestore-vhost"),
        app_config=config,
        # Without LocalServiceConfig, "local" means a remote filestore-server.
        service_type="local",
        verbose=True,
        kikimr_port=0,
        domain=None,
        secure=True)
    configurator.generate_configs()

    vhost = FilestoreVhost(configurator)
    vhost.start()
    try:
        wait_for_filestore_vhost(vhost, configurator.port)
        yield configurator
    finally:
        vhost.stop()


def update_certificates(filestore):
    with CreateClient(f"localhost:{filestore.port}") as client:
        return client.execute_action(
            action="updatecertificates",
            input=b"").Output


def update_vhost_certificates(vhost):
    with CreateEndpointClient(f"localhost:{vhost.port}") as client:
        return client.execute_action(
            action="updatecertificates",
            input=b"").Output


def test_should_update_certificates(filestore, certs):
    port = filestore.secure_port
    cert = get_server_certificate(port, "server.crt")
    old_metrics = get_certificate_metrics(filestore, certs)
    assert old_metrics.expire_ts == get_expire_ts(cert)

    certs.install(
        cert="server_new.crt",
        key="server_new.key",
        root_certs="server_new.crt")

    # The files are not read on their own.
    get_server_certificate(port, "server.crt")
    assert get_certificate_metrics(filestore, certs) == old_metrics

    assert json.loads(update_certificates(filestore)) == {}

    cert = get_server_certificate(port, "server_new.crt")
    metrics = get_certificate_metrics(filestore, certs)
    assert metrics.cert_version != old_metrics.cert_version
    assert metrics.root_version != old_metrics.root_version
    # Both files hold the same certificate now.
    assert metrics.cert_version == metrics.root_version
    assert metrics.expire_ts == get_expire_ts(cert)
    assert metrics.expire_ts != old_metrics.expire_ts

    # The new root certificate is in use too: the client certificate is
    # verified against it.
    with CreateClient(
            f"localhost:{port}",
            credentials=new_credentials(),
            retry_timeout=10) as client:
        client.ping()


def test_should_keep_certificates_that_fail_validation(filestore, certs):
    port = filestore.secure_port
    old_metrics = get_certificate_metrics(filestore, certs)

    # The new certificate does not match the private key.
    certs.install(
        cert="server_new.crt",
        key="server.key",
        root_certs="server.crt")

    with pytest.raises(ClientError) as e:
        update_certificates(filestore)
    assert e.value.code == EResult.E_INVALID_STATE.value

    get_server_certificate(port, "server.crt")
    assert get_certificate_metrics(filestore, certs) == old_metrics

    # Once the files are fixed, the next update applies them.
    certs.install(
        cert="server_new.crt",
        key="server_new.key",
        root_certs="server.crt")

    update_certificates(filestore)

    cert = get_server_certificate(port, "server_new.crt")
    metrics = get_certificate_metrics(filestore, certs)
    assert metrics.cert_version != old_metrics.cert_version
    assert metrics.root_version == old_metrics.root_version
    assert metrics.expire_ts == get_expire_ts(cert)
    assert metrics.expire_ts != old_metrics.expire_ts


def test_should_update_vhost_certificates(vhost, certs, client_certs):
    port = vhost.secure_port
    cert = get_server_certificate(port, "server.crt")
    old_metrics = get_certificate_metrics(vhost, certs)
    assert old_metrics.expire_ts == get_expire_ts(cert)
    # The client files hold the same certificates.
    assert get_certificate_metrics(vhost, client_certs) == old_metrics

    for files in [certs, client_certs]:
        files.install(
            cert="server_new.crt",
            key="server_new.key",
            root_certs="server_new.crt")

    # The files are not read on their own.
    get_server_certificate(port, "server.crt")
    assert get_certificate_metrics(vhost, certs) == old_metrics
    assert get_certificate_metrics(vhost, client_certs) == old_metrics

    cli = FilestoreCliClient(
        binary_path=yatest_common.binary_path(
            "cloud/filestore/apps/client/filestore-client"),
        port=None,
        vhost_port=vhost.port)
    output = cli.execute_action("updatecertificates", {}, vhost=True)
    assert json.loads(output) == {}

    cert = get_server_certificate(port, "server_new.crt")
    metrics = get_certificate_metrics(vhost, certs)
    assert metrics.cert_version != old_metrics.cert_version
    assert metrics.root_version != old_metrics.root_version
    assert metrics.cert_version == metrics.root_version
    assert metrics.expire_ts == get_expire_ts(cert)
    assert metrics.expire_ts != old_metrics.expire_ts
    # The client files are read by the same update.
    assert get_certificate_metrics(vhost, client_certs) == metrics

    with CreateEndpointClient(
            f"localhost:{port}",
            credentials=new_credentials(),
            retry_timeout=10) as client:
        client.ping()


def test_should_keep_vhost_client_certificates_that_fail_validation(
        vhost,
        certs,
        client_certs):
    port = vhost.secure_port
    old_metrics = get_certificate_metrics(vhost, certs)
    old_client_metrics = get_certificate_metrics(vhost, client_certs)

    # The new client certificate does not match the private key.
    client_certs.install(
        cert="server_new.crt",
        key="server.key",
        root_certs="server.crt")

    with pytest.raises(ClientError) as e:
        update_vhost_certificates(vhost)
    assert e.value.code == EResult.E_INVALID_STATE.value

    assert get_certificate_metrics(vhost, client_certs) == old_client_metrics

    # Once the files are fixed, the next update applies them.
    client_certs.install(
        cert="server_new.crt",
        key="server_new.key",
        root_certs="server.crt")

    assert json.loads(update_vhost_certificates(vhost)) == {}

    metrics = get_certificate_metrics(vhost, client_certs)
    assert metrics.cert_version != old_client_metrics.cert_version
    assert metrics.root_version == old_client_metrics.root_version
    assert metrics.expire_ts != old_client_metrics.expire_ts

    # The certificates of the vhost itself have not changed.
    get_server_certificate(port, "server.crt")
    assert get_certificate_metrics(vhost, certs) == old_metrics
