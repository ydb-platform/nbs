GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(BSD)

GO_SKIP_TESTS(
    TestIndex
    TestName
    TestParse
    TestScalingHintingFull
    TestScalingHintingNone
)

SRCS(
    face.go
    glyph.go
    hint.go
    opcodes.go
    truetype.go
)

GO_TEST_SRCS(
    face_test.go
    hint_test.go
    truetype_test.go
)

END()

RECURSE(
    gotest
)
