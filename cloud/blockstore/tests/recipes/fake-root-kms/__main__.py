import argparse
import json
import os

import yatest.common as common
import yatest.common.network as network

from cloud.tasks.test.common.processes import register_process, kill_processes

from library.python.testing.recipe import declare_recipe, set_env


SERVICE_NAME = "fake_root_kms"
HEALTHY_PROBE_KEY_ID = "healthy-probe"


def start(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--with-hanging-backend", action="store_true")
    args = parser.parse_args(argv)

    pm = network.PortManager()
    port = pm.get_port()

    binary_path = common.binary_path(
        "cloud/blockstore/tools/testing/fake-root-kms/fake-root-kms")

    try:
        test_name = common.context.test_name or ''
        test_name = test_name.translate(str.maketrans({':': '_', '/': '_'}))
    except AttributeError:
        test_name = ''

    working_dir = os.path.join(common.output_path(), test_name, "root_kms")
    os.makedirs(working_dir, exist_ok=True)

    certs_dir = common.source_path(
        'cloud/blockstore/tests/recipes/fake-root-kms/certs')

    ca = os.path.join(certs_dir, 'ca.crt')

    nbs_key = os.path.join(certs_dir, 'nbs.key')
    keys = {
        'nbs': nbs_key,
    }
    if args.with_hanging_backend:
        keys[HEALTHY_PROBE_KEY_ID] = nbs_key

    config = {
        'port': port,
        'ca': ca,
        'server_cert': os.path.join(certs_dir, 'server.crt'),
        'server_key': os.path.join(certs_dir, 'server.key'),
        'keys': keys,
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

    register_process(SERVICE_NAME, root_kms.process.pid)

    set_env("FAKE_ROOT_KMS_PORT", str(port))

    if args.with_hanging_backend:
        hanging_port = pm.get_port()
        hanging_config = {
            'port': hanging_port,
            'ca': ca,
            'server_cert': os.path.join(certs_dir, 'server.crt'),
            'server_key': os.path.join(certs_dir, 'server.key'),
            'keys': {
                'nbs': nbs_key,
            },
            'hang_decrypt_requests': True,
        }
        hanging_config_path = os.path.join(
            working_dir,
            "hanging_config.txt",
        )
        with open(hanging_config_path, "w") as f:
            json.dump(hanging_config, f)

        hanging_root_kms = common.execute(
            command=[
                binary_path,
                '--config-path',
                hanging_config_path,
            ],
            cwd=working_dir,
            stdout=os.path.join(working_dir, "hanging_out.txt"),
            stderr=os.path.join(working_dir, "hanging_err.txt"),
            wait=False,
        )
        register_process(SERVICE_NAME, hanging_root_kms.process.pid)
        set_env("FAKE_ROOT_KMS_HANGING_PORT", str(hanging_port))

    set_env("FAKE_ROOT_KMS_CA", ca)
    set_env("FAKE_ROOT_KMS_CLIENT_CRT", os.path.join(certs_dir, 'client.crt'))
    set_env("FAKE_ROOT_KMS_CLIENT_KEY", os.path.join(certs_dir, 'client.key'))


def stop(argv):
    kill_processes(SERVICE_NAME)


if __name__ == "__main__":
    declare_recipe(start, stop)
