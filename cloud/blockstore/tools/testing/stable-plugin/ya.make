UNION()

IF (SANITIZER_TYPE == "thread")
    SET(RESOURCE_ID 4399070549)
    SET(DEB_FILE yandex-cloud-blockstore-plugin_1723132229.stable-23-1_amd64.deb)
ELSE()
    SET(RESOURCE_ID 3240550068)
    SET(DEB_FILE yandex-cloud-blockstore-plugin_1351636689.releases.ydb.stable-22-2_amd64.deb)
ENDIF()

FROM_SANDBOX(
    ${RESOURCE_ID}
    OUT_NOAUTO ${DEB_FILE}
)

RUN_PYTHON3(
    ${CURDIR}/extract.py ${DEB_FILE}
    CWD ${BINDIR}
    IN_NOPARSE ${DEB_FILE}
    OUT_NOAUTO libblockstore-plugin.so
)

END()
