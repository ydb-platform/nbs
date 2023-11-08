PACKAGE()

# generate image using
# >> ya make cloud/storage/core/tools/testing/qemu/build-image
# >> cloud/storage/core/tools/testing/qemu/build-image/build-image \
#     --user qemu --plain-pwd \
#     --ssh-key cloud/storage/core/tools/testing/qemu/keys/id_rsa \
#     --with-rdma

FROM_SANDBOX(
    FILE
    5074323334
    RENAME RESOURCE
    OUT_NOAUTO rootfs.img
)

END()
