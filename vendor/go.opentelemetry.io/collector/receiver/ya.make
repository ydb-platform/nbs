GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    receiver.go
)

GO_TEST_SRCS(receiver_test.go)

END()

RECURSE(
    gotest
    receivertest
    scrapererror
    scraperhelper
)
