LIBRARY()

LICENSE_RESTRICTION_EXCEPTIONS(
    contrib/restricted/libnl/lib/nl-3
    contrib/restricted/libnl/lib/nl-genl-3
)

SRCS(
    message.cpp
    socket.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    contrib/restricted/libnl/lib/nl-3
    contrib/restricted/libnl/lib/nl-genl-3
)

END()
