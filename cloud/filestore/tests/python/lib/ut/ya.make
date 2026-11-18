PY3TEST()

TEST_SRCS(
    client_ut.py
    daemon_config_ut.py
    mount_recipe_ut.py
)

DATA(arcadia/cloud/filestore/tests/recipes/mount/__main__.py)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/tests/python/lib

    contrib/python/protobuf
    contrib/ydb/core/protos
    library/python/testing/recipe
)

END()
