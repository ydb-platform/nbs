GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    route.pb.go
    route.pb.validate.go
    route_components.pb.go
    route_components.pb.validate.go
    scoped_route.pb.go
    scoped_route.pb.validate.go
)

END()
