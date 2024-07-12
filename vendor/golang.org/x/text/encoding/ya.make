GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD-3-Clause)

SRCS(
    encoding.go
)

GO_XTEST_SRCS(
    encoding_test.go
    example_test.go
)

END()

RECURSE(
    charmap
    gotest
    htmlindex
    ianaindex
    internal
    japanese
    korean
    simplifiedchinese
    traditionalchinese
    unicode
)
