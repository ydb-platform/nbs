GO_PROGRAM(access-service-mock)

SRCS(
    main.go
)

END()

RECURSE(
    access_service
    config
    protos
)
