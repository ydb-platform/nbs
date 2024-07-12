GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    aws_iam.pb.go
    aws_iam.pb.validate.go
    file_based_metadata.pb.go
    file_based_metadata.pb.validate.go
)

END()
