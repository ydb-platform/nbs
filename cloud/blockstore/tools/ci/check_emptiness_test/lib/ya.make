PY3_LIBRARY()

PY_SRCS(
    __init__.py
    main.py
)

PEERDIR(
    cloud/blockstore/pylibs/clusters/test_config
    cloud/blockstore/pylibs/common
    cloud/blockstore/pylibs/ycp
)

END()
