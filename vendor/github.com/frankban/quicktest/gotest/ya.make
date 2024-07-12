GO_TEST_FOR(vendor/github.com/frankban/quicktest)

SUBSCRIBER(g:go-contrib)

LICENSE(MIT)

GO_SKIP_TESTS(
    TestReportOutput
    TestIndirectReportOutput
    TestMultilineReportOutput
    TestTopLevelAssertReportOutput
    TestCmpReportOutput
)

IF (GOSTD_VERSION MATCHES "1.13.")
    # Do nothing
ELSE()
    GO_SKIP_TESTS(TestCDeferCalledEvenAfterDeferPanic)
ENDIF()

END()
