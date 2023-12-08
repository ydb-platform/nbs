LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Service-Dll-Harness)

VERSION(3.2)

BUILD_ONLY_IF(OS_LINUX)

NO_UTIL()

NO_RUNTIME()

IF (USE_DYNAMIC_LIBFUSE)
    PEERDIR(
        contrib/libs/virtiofsd/dynamic
    )
ELSE()
    PEERDIR(
        contrib/libs/virtiofsd/static
    )
ENDIF()

END()

RECURSE(
    dynamic
    static
)
