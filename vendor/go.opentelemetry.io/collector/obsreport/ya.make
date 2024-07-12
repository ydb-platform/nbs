GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    doc.go
    obsreport.go
    obsreport_exporter.go
    obsreport_processor.go
    obsreport_receiver.go
    obsreport_scraper.go
)

GO_TEST_SRCS(obsreport_test.go)

END()

RECURSE(
    gotest
    obsreporttest
)
