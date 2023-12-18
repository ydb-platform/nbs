GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    alter_topic.go
    client.go
    controlplane_types.go
    create_topic.go
    describe_topic.go
    drop_topic.go
)

END()

RECURSE(
    rawtopiccommon
    rawtopicreader
    rawtopicwriter
)
