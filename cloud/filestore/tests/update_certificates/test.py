"""Manual TLS certificate updates with the "updatecertificates" action.

Periodic certificate checks are disabled, so filestore-server applies new
certificate files only when the action asks for it.
"""

import json
import os
import shutil
import socket
import ssl
import tempfile

import pytest

import yatest.common as yatest_common

from cloud.filestore.config.server_pb2 import TNullServiceConfig, \
    TServerAppConfig, TServerConfig
from cloud.filestore.public.sdk.python.client import ClientCredentials, \
    ClientError, CreateClient, EResult
from cloud.filestore.tests.python.lib.daemon_config import \
    FilestoreServerConfigGenerator
from cloud.filestore.tests.python.lib.server import FilestoreServer, \
    wait_for_filestore_server


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


def get_server_certificate(port):
    """Returns the certificate the server presents in a new TLS handshake."""

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.set_alpn_protocols(["h2"])

    with socket.create_connection(("localhost", port)) as sock:
        with context.wrap_socket(sock, server_hostname="localhost") as tls:
            return tls.getpeercert(binary_form=True)


@pytest.fixture
def certs():
    files = CertificateFiles()
    files.install(
        cert="server.crt",
        key="server.key",
        root_certs="server.crt")
    return files


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

    server = FilestoreServer(configurator=configurator)
    server.start()
    try:
        wait_for_filestore_server(server, configurator.port)
        yield configurator
    finally:
        server.stop()


def update_certificates(filestore):
    with CreateClient(f"localhost:{filestore.port}") as client:
        return client.execute_action(
            action="updatecertificates",
            input=b"").Output


def test_should_update_certificates(filestore, certs):
    port = filestore.secure_port
    assert get_server_certificate(port) == read_certificate("server.crt")

    certs.install(
        cert="server_new.crt",
        key="server_new.key",
        root_certs="server_new.crt")

    # The files are not read on their own.
    assert get_server_certificate(port) == read_certificate("server.crt")

    assert json.loads(update_certificates(filestore)) == {}

    assert get_server_certificate(port) == read_certificate("server_new.crt")

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


def test_should_keep_certificates_that_fail_validation(filestore, certs):
    port = filestore.secure_port

    # The new certificate does not match the private key.
    certs.install(
        cert="server_new.crt",
        key="server.key",
        root_certs="server.crt")

    with pytest.raises(ClientError) as e:
        update_certificates(filestore)
    assert e.value.code == EResult.E_INVALID_STATE.value

    assert get_server_certificate(port) == read_certificate("server.crt")

    # Once the files are fixed, the next update applies them.
    certs.install(
        cert="server_new.crt",
        key="server_new.key",
        root_certs="server.crt")

    update_certificates(filestore)

    assert get_server_certificate(port) == read_certificate("server_new.crt")
