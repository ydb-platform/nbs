OWNER(g:cloud-nbs)

GO_PROGRAM(acceptance-test)

SRCS(
    main.go
    ycp.go
)

END()

RECURSE(
    cmp
    test_runner
)
