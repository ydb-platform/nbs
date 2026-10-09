RECURSE(
    acceptance
    filestore_client
    images
    mocks
    recipe
    remote
    snapshot_backup
)

RECURSE_FOR_TESTS(
    snapshot_migration_test
)
