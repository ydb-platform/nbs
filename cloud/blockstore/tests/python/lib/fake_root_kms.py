import json
import os
import socket

import yatest.common as common


def start_fake_root_kms(working_dir, port):
    binary_path = common.binary_path(
        "cloud/blockstore/tools/testing/fake-root-kms/fake-root-kms")
    os.makedirs(working_dir, exist_ok=True)

    certs_dir = common.source_path(
        'cloud/blockstore/tests/recipes/fake-root-kms/certs')

    ca = os.path.join(certs_dir, 'ca.crt')

    config = {
        'port': port,
        'ca': ca,
        'server_cert': os.path.join(certs_dir, 'server.crt'),
        'server_key': os.path.join(certs_dir, 'server.key'),
        'keys': {
            'nbs': os.path.join(certs_dir, 'nbs.key')
        }
    }

    config_path = os.path.join(working_dir, "config.txt")

    with open(config_path, "w") as f:
        json.dump(config, f)

    root_kms = common.execute(
        command=[binary_path, '--config-path', config_path],
        cwd=working_dir,
        stdout=os.path.join(working_dir, "out.txt"),
        stderr=os.path.join(working_dir, "err.txt"),
        wait=False,
    )

    environment = {
        "FAKE_ROOT_KMS_PORT": str(port),
        "FAKE_ROOT_KMS_CA": ca,
        "FAKE_ROOT_KMS_CLIENT_CRT": os.path.join(certs_dir, 'client.crt'),
        "FAKE_ROOT_KMS_CLIENT_KEY": os.path.join(certs_dir, 'client.key'),
    }
    return root_kms, environment


def wait_for_fake_root_kms(process, port):
    def ready():
        if not process.running:
            process.wait()
            raise RuntimeError("fake-root-kms exited before becoming ready")
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return True
        except OSError:
            return False

    common.wait_for(ready, timeout=30)
