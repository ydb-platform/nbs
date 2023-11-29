PY3_PROGRAM(yc-storage-breakpad-sender)

PEERDIR(
    cloud/storage/core/tools/breakpad
)

PY_MAIN(cloud.storage.core.tools.breakpad.crash_processor:main)

END()
