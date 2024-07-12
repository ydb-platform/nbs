GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    templates.go
)

GO_TEST_SRCS(templates_test.go)

GO_EMBED_PATTERN(templates/component_header.html)

GO_EMBED_PATTERN(templates/extensions_table.html)

GO_EMBED_PATTERN(templates/features_table.html)

GO_EMBED_PATTERN(templates/page_footer.html)

GO_EMBED_PATTERN(templates/page_header.html)

GO_EMBED_PATTERN(templates/pipelines_table.html)

GO_EMBED_PATTERN(templates/properties_table.html)

END()

RECURSE(
    gotest
)
