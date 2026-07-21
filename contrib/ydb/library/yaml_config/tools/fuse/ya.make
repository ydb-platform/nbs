PROGRAM(yaml-config-fuse)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/library/yaml_config/public
    contrib/ydb/library/fyamlcpp
    library/cpp/getopt
)

END()
