PY3_LIBRARY()

PY_SRCS(
    __init__.py
    ycp.py
    ycp_wrapper.py
)

RESOURCE(
    fake-responses/fake-disk.json                        fake-disk.json
    fake-responses/fake-disk-list.json                   fake-disk-list.json
    fake-responses/fake-disk-placement-group.json        fake-disk-placement-group.json
    fake-responses/fake-disk-placement-group-list.json   fake-disk-placement-group-list.json
    fake-responses/fake-filesystem.json                  fake-filesystem.json
    fake-responses/fake-filesystem-list.json             fake-filesystem-list.json
    fake-responses/fake-image-list.json                  fake-image-list.json
    fake-responses/fake-iam-token.json                   fake-iam-token.json
    fake-responses/fake-instance.json                    fake-instance.json
    fake-responses/fake-instance-list.json               fake-instance-list.json
    fake-responses/fake-placement-group.json             fake-placement-group.json
    fake-responses/fake-placement-group-list.json        fake-placement-group-list.json
    fake-responses/fake-subnet-list.json                 fake-subnet-list.json
)

PEERDIR(
    contrib/python/python-dateutil
    contrib/python/Jinja2

    cloud/blockstore/pylibs/clusters/test_config
    cloud/blockstore/pylibs/common

    library/python/resource
)

END()
