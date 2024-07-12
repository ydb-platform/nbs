GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    doc.go
)

END()

RECURSE(
    planmodifiers
    testplanmodifier
    testprovider
    testschema
    testvalidator
    types
)
