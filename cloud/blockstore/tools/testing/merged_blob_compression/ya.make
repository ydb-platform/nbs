PY3_LIBRARY()

PY_SRCS(reader_fence.py)

END()

RECURSE(bench workload)
RECURSE_FOR_TESTS(tests)
