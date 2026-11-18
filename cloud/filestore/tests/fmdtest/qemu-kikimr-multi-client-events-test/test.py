from cloud.filestore.tests.fmdtest.lib.multi_client import (
    run_multi_client_test,
)


def test_create_unlink_rename_check():
    run_multi_client_test(
        "create_unlink_rename_check",
        instance_count=2,
        rounds=20,
        files_per_round=100)
