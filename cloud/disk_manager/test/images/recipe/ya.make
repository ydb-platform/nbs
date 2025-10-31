PY3_PROGRAM()

PY_SRCS(
    __main__.py
    common.py
    image_file_server_launcher.py
    raw_image_generator.py
    vmdk_image_generator.py
)

PEERDIR(
    cloud/storage/core/tools/common/python
    cloud/tasks/test/common
    ydb/tests/library
    library/python/testing/recipe
)

END()
