module go.opentelemetry.io/collector/consumer

go 1.20

require (
    github.com/stretchr/testify v1.8.4
    go.opentelemetry.io/collector v0.85.0
    go.opentelemetry.io/collector/pdata v1.0.0-rcv0014
)

require (
    github.com/davecgh/go-spew v1.1.1 // indirect
    github.com/gogo/protobuf v1.3.2 // indirect
    github.com/golang/protobuf v1.5.3 // indirect
    github.com/json-iterator/go v1.1.12 // indirect
    github.com/kr/text v0.2.0 // indirect
    github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
    github.com/modern-go/reflect2 v1.0.2 // indirect
    github.com/pmezard/go-difflib v1.0.0 // indirect
    go.uber.org/multierr v1.11.0 // indirect
    golang.org/x/net v0.15.0 // indirect
    golang.org/x/sys v0.12.0 // indirect
    golang.org/x/text v0.13.0 // indirect
    google.golang.org/genproto/googleapis/rpc v0.0.0-20230530153820-e85fd2cbaebc // indirect
    google.golang.org/grpc v1.57.0 // indirect
    google.golang.org/protobuf v1.31.0 // indirect
    gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace go.opentelemetry.io/collector => ../

replace go.opentelemetry.io/collector/component => ../component

replace go.opentelemetry.io/collector/confmap => ../confmap

replace go.opentelemetry.io/collector/exporter => ../exporter

replace go.opentelemetry.io/collector/extension => ../extension

replace go.opentelemetry.io/collector/extension/zpagesextension => ../extension/zpagesextension

replace go.opentelemetry.io/collector/featuregate => ../featuregate

replace go.opentelemetry.io/collector/pdata => ../pdata

replace go.opentelemetry.io/collector/processor => ../processor

replace go.opentelemetry.io/collector/receiver => ../receiver

replace go.opentelemetry.io/collector/semconv => ../semconv

retract (
    v0.76.0 // Depends on retracted pdata v1.0.0-rc10 module, use v0.76.1
    v0.69.0 // Release failed, use v0.69.1
)

replace go.opentelemetry.io/collector/connector => ../connector

replace go.opentelemetry.io/collector/config/confignet => ../config/confignet

replace go.opentelemetry.io/collector/config/configtelemetry => ../config/configtelemetry
