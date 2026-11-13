"""Manual TLS certificate updates with the "updatecertificates" action.

Periodic certificate checks are disabled, so nbsd applies new certificate
files only when the action asks for it.
"""

import collections
import json
import os
import shutil
import socket
import ssl
import tempfile

import pytest

import yatest.common as yatest_common

from cloud.blockstore.config.server_pb2 import TNullServiceConfig, \
    TServerAppConfig, TServerConfig
from cloud.blockstore.public.sdk.python.client import ClientCredentials, \
    ClientError, CreateClient, EResult
from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import get_nbs_counters, \
    get_sensor, thread_count, wait_for_nbs_server


CERTS_DIR = "cloud/blockstore/tests/certs"


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


def get_certificate_metrics(nbs, certs):
    """Returns what the metrics report for the files in use: the versions of
    the certificate and the root certificates and the expiration time of the
    certificate."""

    sensors = get_nbs_counters(nbs.mon_port)["sensors"]

    def get(path, name):
        value = get_sensor(
            sensors,
            None,
            component="certificates",
            path=path,
            sensor=name)
        assert value is not None, f"No {name} metric for {path}"
        return value

    return CertificateMetrics(
        cert_version=get(certs.cert_file, "Fingerprint"),
        root_version=get(certs.root_certs_file, "Fingerprint"),
        expire_ts=get(certs.cert_file, "ExpireTs"))


@pytest.fixture
def certs():
    files = CertificateFiles()
    files.install(
        cert="server.crt",
        key="server.key",
        root_certs="server.crt")
    return files


@pytest.fixture
def nbs(certs):
    server = TServerAppConfig()
    server.NullServiceConfig.CopyFrom(TNullServiceConfig())
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.RootCertsFile = certs.root_certs_file
    cert = server.ServerConfig.Certs.add()
    cert.CertFile = certs.cert_file
    cert.CertPrivateKeyFile = certs.key_file
    server.ServerConfig.RefreshCertsPeriod = 0

    nbs = LocalNbs(
        0,
        server_app_config=server,
        enable_tls=True,
        nbs_binary_path=yatest_common.binary_path(
            "cloud/blockstore/apps/server_lightweight/nbsd-lightweight"))
    nbs.start()
    try:
        wait_for_nbs_server(nbs.nbs_port)
        yield nbs
    finally:
        nbs.stop()


def update_certificates(nbs):
    with CreateClient(f"localhost:{nbs.nbs_port}") as client:
        return client.execute_action(
            action="updatecertificates",
            input_bytes=b"")


def test_should_update_certificates(nbs, certs):
    port = nbs.nbs_secure_port
    cert = get_server_certificate(port, "server.crt")
    old_metrics = get_certificate_metrics(nbs, certs)
    assert old_metrics.expire_ts == get_expire_ts(cert)

    certs.install(
        cert="server_new.crt",
        key="server_new.key",
        root_certs="server_new.crt")

    # The files are not read on their own.
    get_server_certificate(port, "server.crt")
    assert get_certificate_metrics(nbs, certs) == old_metrics

    assert json.loads(update_certificates(nbs)) == {}

    cert = get_server_certificate(port, "server_new.crt")
    metrics = get_certificate_metrics(nbs, certs)
    assert metrics.cert_version != old_metrics.cert_version
    assert metrics.root_version != old_metrics.root_version
    # Both files hold the same certificate now.
    assert metrics.cert_version == metrics.root_version
    assert metrics.expire_ts == get_expire_ts(cert)
    assert metrics.expire_ts != old_metrics.expire_ts

    # The new root certificate is in use too: the client certificate is
    # verified against it.
    credentials = ClientCredentials(
        root_certs_file=source_file("server_new.crt"),
        cert_file=source_file("server_new.crt"),
        cert_private_key_file=source_file("server_new.key"))
    with CreateClient(
            f"localhost:{port}",
            credentials=credentials,
            retry_timeout=10) as client:
        client.ping()


def test_should_keep_certificates_that_fail_validation(nbs, certs):
    port = nbs.nbs_secure_port
    old_metrics = get_certificate_metrics(nbs, certs)

    # The new certificate does not match the private key.
    certs.install(
        cert="server_new.crt",
        key="server.key",
        root_certs="server.crt")

    with pytest.raises(ClientError) as e:
        update_certificates(nbs)
    assert e.value.code == EResult.E_INVALID_STATE.value

    get_server_certificate(port, "server.crt")
    assert get_certificate_metrics(nbs, certs) == old_metrics

    # Once the files are fixed, the next update applies them.
    certs.install(
        cert="server_new.crt",
        key="server_new.key",
        root_certs="server.crt")

    update_certificates(nbs)

    cert = get_server_certificate(port, "server_new.crt")
    metrics = get_certificate_metrics(nbs, certs)
    assert metrics.cert_version != old_metrics.cert_version
    assert metrics.root_version == old_metrics.root_version
    assert metrics.expire_ts == get_expire_ts(cert)
    assert metrics.expire_ts != old_metrics.expire_ts
