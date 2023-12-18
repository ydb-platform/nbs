PY3_PROGRAM(yc-nfs-ci-build-arcadia-test)

PY_SRCS(
    __main__.py
    coreutils.py
    common.py
    entrypoint.py
)

PEERDIR(
    cloud/blockstore/pylibs/clusters/test_config
    cloud/blockstore/pylibs/common
    cloud/blockstore/pylibs/ycp
)

RESOURCE(
    scripts/coreutils.sh coreutils.sh
)

END()

