PACKAGE()

IF(USE_SYSTEM_VIRTIOFSD)
    RUN_PYTHON3(
        cp.py /usr/lib/qemu/virtiofsd virtiofs-server
        OUT virtiofs-server
    )
ELSE()

FROM_SANDBOX(
    FILE
    4556399018
    RENAME RESOURCE
    OUT_NOAUTO virtiofs-server
    EXECUTABLE)

ENDIF()

END()
