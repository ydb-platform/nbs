PY2_PROGRAM(yc-storage-breakpad-launcher)

ALLOCATOR(TCMALLOC_TC)

PEERDIR(
    cloud/storage/core/tools/breakpad/common
)

PY_SRCS(
    core_checker.py
    error_collector.py
    launcher.py
    oom_checker.py
)

PY_MAIN(cloud.storage.core.tools.breakpad.launcher.launcher:main)

END()
