LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Service-Dll-Harness)

VERSION(2.9.3)

BUILD_ONLY_IF(OS_LINUX)

NO_RUNTIME()

NO_COMPILER_WARNINGS()

IF (USE_DYNAMIC_LIBFUSE)
    PEERDIR(
        contrib/libs/fuse/dynamic
    )
ELSE()
    PEERDIR(
        contrib/libs/fuse/static
    )
ENDIF()

END()

RECURSE(
    dynamic
    static
)
