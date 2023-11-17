PY3_LIBRARY()

PY_SRCS(
    __init__.py
    errors.py
    main.py
    results_processor_fs.py
    test_cases.py
)

PEERDIR(
    cloud/blockstore/pylibs/clusters/test_config
    cloud/blockstore/pylibs/common
    cloud/blockstore/pylibs/ycp
)

END()
