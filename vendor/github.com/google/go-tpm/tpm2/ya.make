GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    audit.go
    bitfield.go
    constants.go
    constants_internal.go
    crypto.go
    errors.go
    kdf.go
    marshalling.go
    names.go
    policy.go
    reflect.go
    sessions.go
    structures.go
    templates.go
    tpm2.go
    tpm2b.go
)

END()

RECURSE(
    transport
)
