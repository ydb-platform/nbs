import pytest
import os

import cloud.storage.core.tools.testing.fio.lib as fio

from cloud.filestore.tests.python.lib.client import make_socket_generator
from cloud.storage.core.tools.testing.qemu.lib.qemu_with_migration import QemuWithMigration

TESTS = fio.generate_default_index_test()


socket_generator = make_socket_generator(
    os.getenv("NFS_SERVER_PORT"),
    os.getenv("NFS_VHOST_PORT"),
    endpoint_storage_dir=os.getenv("NFS_VHOST_ENDPOINT_STORAGE_DIR", None))
migration = QemuWithMigration(socket_generator)


@pytest.mark.parametrize("name", TESTS.keys())
def test_fio(name):

    migration.start()
    migration.migrate(2, 20)

    return None
