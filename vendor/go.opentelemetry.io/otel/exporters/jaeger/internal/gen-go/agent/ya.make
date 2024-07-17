GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    GoUnusedProtection__.go
    agent-consts.go
    agent.go
)

END()

RECURSE(
    agent-remote
)
