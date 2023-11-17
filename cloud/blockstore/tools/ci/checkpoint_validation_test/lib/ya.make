PY3_LIBRARY()

RESOURCE(
    configs/nbs-client.txt nbs-client.txt
    configs/throttler-profile.txt throttler-profile.txt
)

PY_SRCS(
    __init__.py
    errors.py
    main.py
    test_cases.py
)

PEERDIR(
    cloud/blockstore/public/sdk/python/client

    cloud/blockstore/pylibs/common
    cloud/blockstore/pylibs/sdk
    cloud/blockstore/pylibs/ycp
)

END()

