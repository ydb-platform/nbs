DEFAULT(USE_SYSTEMD ${_USE_SYSTEMD})

LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(250.12)

ORIGINAL_SOURCE(https://github.com/systemd/systemd-stable/archive/v250.12.tar.gz)

LICENSE(Service-Dll-Harness)

NO_RUNTIME()

IF (USE_SYSTEMD == "dynamic")
    PEERDIR(
        contrib/libs/systemd/dynamic
    )
ELSEIF (USE_SYSTEMD == "local")
    GLOBAL_CFLAGS(${USE_LOCAL_SYSTEMD_CFLAGS})
    DEFAULT(USE_LOCAL_SYSTEMD_LDFLAGS -lsystemd)
    LDFLAGS(${USE_LOCAL_SYSTEMD_LDFLAGS})

    # CFLAGS(
    #     -I/usr/include
    # )
    # LDFLAGS(
    #     -L/usr/lib/x86_64-linux-gnu
        # -l:libnl-3.a
        # -l:libnl-genl-3.a
    # )
ELSE()
    PEERDIR(
        contrib/libs/systemd/static
    )
ENDIF()

END()

IF (USE_SYSTEMD != "local")
    RECURSE(
        dynamic
        static
    )
ENDIF()