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

RESOURCE(
    default.sh default.sh
    generic_000-099.sh generic_000-099.sh
    generic_100-199.sh generic_100-199.sh
    generic_200-299.sh generic_200-299.sh
    generic_300-399.sh generic_300-399.sh
    generic_400-499.sh generic_400-499.sh
    generic_500-599.sh generic_500-599.sh
    generic_600-699.sh generic_600-699.sh
    generic_700-799.sh generic_700-799.sh
)

END()
