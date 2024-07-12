GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(Apache-2.0)

SRCS(
    attestation.pb.go
    build.pb.go
    common.pb.go
    compliance.pb.go
    cvss.pb.go
    deployment.pb.go
    discovery.pb.go
    dsse_attestation.pb.go
    grafeas.pb.go
    image.pb.go
    intoto_provenance.pb.go
    intoto_statement.pb.go
    package.pb.go
    provenance.pb.go
    severity.pb.go
    slsa_provenance.pb.go
    slsa_provenance_zero_two.pb.go
    upgrade.pb.go
    vex.pb.go
    vulnerability.pb.go
)

END()
