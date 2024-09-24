OWNER(g:cloud-nbs)

IF (BUILD_CSI_DRIVER)

RECURSE(
    client
    cmd
    internal
)

ENDIF()
