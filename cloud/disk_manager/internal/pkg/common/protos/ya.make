OWNER(g:cloud-nbs)

PROTO_LIBRARY()

INCLUDE_TAGS(GO_PROTO)
EXCLUDE_TAGS(JAVA_PROTO)

SRCS(
    ints.proto
    string_map.proto
    strings.proto
)

END()
