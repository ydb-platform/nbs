PY3_PROGRAM(ydb-dstool)

STRIP()

PY_MAIN(contrib.ydb.apps.dstool.main)

PY_SRCS(
    main.py
)

PEERDIR(
    contrib/ydb/apps/dstool/lib
)

END()
