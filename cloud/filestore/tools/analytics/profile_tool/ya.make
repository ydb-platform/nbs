PROGRAM(filestore-profile-tool)

PEERDIR(
    cloud/filestore/tools/analytics/libs/event-log
    cloud/filestore/tools/analytics/profile_tool/lib

    library/cpp/eventlog/dumper
)

SRCS(
    main.cpp
)

END()

RECURSE(
    lib
)
