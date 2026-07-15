PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

TEST_SRCS(
    backtrace_ut.py
    qemu_ut.py
    qmp_ut.py
    recipe_ut.py
)

PEERDIR(
    cloud/storage/core/tools/testing/qemu/lib
)

END()
