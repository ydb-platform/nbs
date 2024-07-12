GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    scraper.go
    scrapercontroller.go
    settings.go
)

GO_TEST_SRCS(
    scrapercontroller_test.go
    settings_test.go
)

END()

RECURSE(
    gotest
)
