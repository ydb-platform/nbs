PY3_LIBRARY()

PY_SRCS(
    __init__.py
    arg_parser.py
    errors.py
    main.py
    test_configs.py
)

PEERDIR(
    cloud/storage/core/tools/testing/fio/lib
)

END()
