PACKAGE()

# customized windows datacenter 2019 image
# login/pswd: Administrator/qemupass!77
FROM_SANDBOX(
    3188763079
    RENAME winx.qcow2
    OUT_NOAUTO win.img
)

END()
