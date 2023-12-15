OWNER(g:cloud-nbs)

PY3_PROGRAM()

PY_SRCS(
    __main__.py
    image_file_server_launcher.py
    vmdk_image_generator.py
)

PEERDIR(
    cloud/disk_manager/test/common
    cloud/storage/core/tools/common/python
    contrib/ydb/tests/library
    library/python/testing/recipe
)

END()
