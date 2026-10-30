RECURSE(
    acceptance
    filestore_client
    images
    mocks
    recipe
    remote
)

RECURSE_FOR_TESTS(
    snapshot_export_test
    snapshot_migration_test
)
