GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    dbus.go
    methods.go
    properties.go
    set.go
    subscription.go
    subscription_set.go
)

GO_TEST_SRCS(
    # dbus_test.go
    # methods_test.go
    # set_test.go
    # subscription_set_test.go
    # subscription_test.go
)

END()

RECURSE(
    #    gotest
)
