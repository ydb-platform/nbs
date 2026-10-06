import os
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

import pytest

from cloud.storage.core.tools.testing.qemu.lib import recipe
from cloud.storage.core.tools.testing.qemu.lib.common import SshToGuest


def test_host_ports_are_forwarded_only_by_the_test_ssh_session():
    args = recipe._parse_args([
        "--forward-host-port-envs", "FAKE_ROOT_KMS_PORT OTHER_PORT",
    ])
    with mock.patch.dict(os.environ, {
        "FAKE_ROOT_KMS_PORT": "23456",
        "OTHER_PORT": "34567",
    }):
        ports = recipe._get_forward_host_ports(args)

    ssh = SshToGuest(user="qemu", port=45678, key="/test/id_rsa")
    command = ssh.get_command(
        "sudo /run_test.sh", wrap_test_env=False, forward_host_ports=ports)

    assert command[-2:] == ["127.0.0.1", "sudo /run_test.sh"]
    assert command[command.index("ExitOnForwardFailure=yes") - 1] == "-o"
    assert [command[i + 1] for i, arg in enumerate(command) if arg == "-R"] == [
        "localhost:23456:localhost:23456",
        "localhost:34567:localhost:34567",
    ]
    assert "-R" not in ssh.get_command("exit 0")


@pytest.mark.parametrize("value", [None, "", "abc", "0", "65536"])
def test_invalid_forwarded_host_port_is_rejected(value):
    args = recipe._parse_args(["--forward-host-port-envs", "FAKE_ROOT_KMS_PORT"])
    with mock.patch.dict(os.environ, {}, clear=True):
        if value is not None:
            os.environ["FAKE_ROOT_KMS_PORT"] = value
        with pytest.raises(recipe.QemuKvmRecipeException, match="FAKE_ROOT_KMS_PORT"):
            recipe._get_forward_host_ports(args)


@pytest.mark.parametrize("argv", [[], ["--forward-host-port-envs", "$QEMU_FORWARD_HOST_PORT_ENVS"]])
def test_host_port_forwarding_is_optional(argv):
    assert recipe._get_forward_host_ports(recipe._parse_args(argv)) == []


def test_recipe_set_env_updates_current_process_and_recipe_env():
    env_name = "QEMU_TEST_VALUE__2"

    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop(env_name, None)
        with mock.patch.object(
            recipe.library.python.testing.recipe,
            "set_env",
        ) as persist_env:
            recipe.recipe_set_env("QEMU_TEST_VALUE", 123, guest_index=2)

        assert os.environ[env_name] == "123"
        persist_env.assert_called_once_with(env_name, "123")


def test_process_coredumps_does_not_fallback_to_port_22():
    args = SimpleNamespace(instance_count=1, ssh_user="qemu")

    with mock.patch.dict(
        os.environ,
        {"QEMU_SSH_KEY": "/test/id_rsa"},
        clear=False,
    ):
        os.environ.pop("QEMU_FORWARDING_PORT", None)
        with mock.patch.object(
            recipe,
            "_process_instance_coredumps",
        ) as process_instance_coredumps:
            recipe._process_coredumps(args)

    process_instance_coredumps.assert_not_called()


def test_process_coredumps_uses_same_process_recipe_env():
    args = SimpleNamespace(instance_count=1, ssh_user="qemu")

    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("QEMU_FORWARDING_PORT", None)
        os.environ.pop("QEMU_SSH_KEY", None)
        with mock.patch.object(
            recipe.library.python.testing.recipe,
            "set_env",
        ):
            recipe.recipe_set_env("QEMU_FORWARDING_PORT", "45757")
            recipe.recipe_set_env("QEMU_SSH_KEY", "/test/id_rsa")

        with mock.patch.object(
            recipe,
            "_process_instance_coredumps",
        ) as process_instance_coredumps:
            recipe._process_coredumps(args)

    process_instance_coredumps.assert_called_once_with(
        user="qemu",
        port=45757,
        key="/test/id_rsa",
    )


@pytest.mark.parametrize("invoke_test", [False, True])
def test_start_instance_sets_up_coredumps_immediately_after_ssh(invoke_test):
    args = SimpleNamespace(
        shared_nic_port=0,
        invoke_test=invoke_test,
        forward_host_port_envs="FAKE_ROOT_KMS_PORT",
    )
    events = []

    ssh = mock.Mock()
    ssh.get_command.return_value = ["ssh", "guest"]
    qemu = mock.Mock()
    qemu.qemu_bin.stderr_file_name = "/test/qemu.err"
    qemu.qemu_bin.daemon.process.pid = 123
    qemu.get_ssh_port.return_value = 45757

    patches = [
        mock.patch.object(recipe, "recipe_set_env"),
        mock.patch.object(recipe, "_get_vm_virtio", return_value="none"),
        mock.patch.object(recipe, "get_mount_paths", return_value=[]),
        mock.patch.object(recipe, "_get_vm_use_virtiofs_server", return_value=False),
        mock.patch.object(recipe, "_get_qemu_kvm", return_value="qemu"),
        mock.patch.object(recipe, "_get_qemu_firmware", return_value="firmware"),
        mock.patch.object(recipe, "_get_qemu_bios", return_value=None),
        mock.patch.object(recipe, "_get_rootfs", return_value="rootfs"),
        mock.patch.object(recipe, "_get_kernel", return_value=None),
        mock.patch.object(recipe, "_get_kcmdline", return_value=None),
        mock.patch.object(recipe, "_get_initrd", return_value=None),
        mock.patch.object(recipe, "_get_vm_mem", return_value="1G"),
        mock.patch.object(recipe, "_get_vm_proc", return_value="1"),
        mock.patch.object(recipe, "_get_qemu_options", return_value=[]),
        mock.patch.object(recipe, "_get_vm_enable_kvm", return_value=False),
        mock.patch.object(recipe, "_get_num_request_queues", return_value=1),
        mock.patch.object(recipe, "_get_chardev_reconnect", return_value=None),
        mock.patch.object(recipe, "_get_virtiofs_migration", return_value=None),
        mock.patch.object(recipe, "Qemu", return_value=qemu),
        mock.patch.object(recipe, "append_recipe_err_files"),
        mock.patch.object(recipe, "_get_ssh_user", return_value="qemu"),
        mock.patch.object(recipe, "_get_ssh_key", return_value="/test/id_rsa"),
        mock.patch.object(recipe, "SshToGuest", return_value=ssh),
        mock.patch.dict(os.environ, {"FAKE_ROOT_KMS_PORT": "23456"}),
        mock.patch.object(
            recipe,
            "_wait_ssh",
            side_effect=lambda *_: events.append("ssh-ready"),
        ),
        mock.patch.object(
            recipe,
            "setup_coredumps",
            side_effect=lambda *_: events.append("coredumps"),
        ),
        mock.patch.object(
            recipe,
            "_prepare_test_environment",
            side_effect=lambda *_: events.append("guest-setup"),
        ),
        mock.patch.object(recipe, "recipe_get_env", return_value=None),
        mock.patch("builtins.open", mock.mock_open()),
    ]

    with ExitStack() as stack:
        for patch in patches:
            stack.enter_context(patch)
        recipe.start_instance(args, 0)

    assert events == ["ssh-ready", "coredumps", "guest-setup"]
    if invoke_test:
        ssh.get_command.assert_called_once_with(
            "sudo /run_test.sh", wrap_test_env=False, forward_host_ports=[23456])
    else:
        ssh.get_command.assert_not_called()
