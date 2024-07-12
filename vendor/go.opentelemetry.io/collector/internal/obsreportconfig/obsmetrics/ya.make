GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    obs_exporter.go
    obs_processor.go
    obs_receiver.go
    obs_scraper.go
    obsmetrics.go
)

END()
