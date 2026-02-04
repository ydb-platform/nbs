LIBRARY()

SRCS(
    tablet_writedata.cpp
    tablet_adddata.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/tablet/events
    cloud/filestore/libs/storage/tablet/model
)

END()
