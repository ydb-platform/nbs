PROGRAM(mask_sensitive_data)

PEERDIR(
    cloud/filestore/libs/diagnostics/events
    library/cpp/digest/md5
    library/cpp/eventlog
    library/cpp/getopt/small
)

SRCS(
    main.cpp
    options.cpp
    mask.cpp
)

END()
