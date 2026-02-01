PY3TEST()

TEST_SRCS(test.py)

RESOURCE(test.txt test.txt)

PEERDIR(
    contrib/ydb/library/benchmarks/template
)

END()
