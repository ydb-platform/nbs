import os
from contextlib import ExitStack
from types import SimpleNamespace
from unittest import mock

from cloud.storage.core.tools.testing.qemu.lib import recipe


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


def test_start_instance_sets_up_coredumps_immediately_after_ssh():
    args = SimpleNamespace(shared_nic_port=0, invoke_test=False)
    events = []

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
        mock.patch.object(recipe, "SshToGuest"),
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
