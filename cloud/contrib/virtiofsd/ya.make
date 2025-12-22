LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(Service-Dll-Harness)

VERSION(3.2)

BUILD_ONLY_IF(OS_LINUX)

NO_UTIL()

NO_RUNTIME()

IF (USE_DYNAMIC_LIBFUSE)
    PEERDIR(
        cloud/contrib/virtiofsd/dynamic
    )
ELSE()
    PEERDIR(
        cloud/contrib/virtiofsd/static
    )
ENDIF()

END()

RECURSE(
    dynamic
    static
)
