OTEL_DOCKER_PROTOBUF ?= otel/build-protobuf:0.11.0
BUF_DOCKER ?= bufbuild/buf:1.7.0

PROTOC := docker run --rm -u ${shell id -u} -v${PWD}:${PWD} -w${PWD} ${OTEL_DOCKER_PROTOBUF} --proto_path=${PWD}/src


PROTO_GEN_GO_DIR ?= gen-go
PROTO_GEN_PYTHON_DIR ?= gen-python
PROTO_GEN_RUBY_DIR ?= gen-ruby
PROTO_GEN_OPENAPI_DIR ?= gen-openapi

# Generate gRPC/Protobuf implementation for Go.
.PHONY: gen-go
gen-go:
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/agent/common/v1/common.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/resource/v1/resource.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/metrics/v1/metrics.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/stats/v1/stats.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/trace/v1/trace.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/trace/v1/trace_config.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/agent/common/v1/common.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/agent/metrics/v1/metrics_service.proto
    $(PROTOC) --go_out=plugins=grpc:./$(PROTO_GEN_GO_DIR) opencensus/proto/agent/trace/v1/trace_service.proto
    $(PROTOC) --grpc-gateway_out=logtostderr=true,grpc_api_configuration=src/opencensus/proto/agent/trace/v1/trace_service_http.yaml:./$(PROTO_GEN_GO_DIR) opencensus/proto/agent/trace/v1/trace_service.proto
    $(PROTOC) --grpc-gateway_out=logtostderr=true,grpc_api_configuration=src/opencensus/proto/agent/metrics/v1/metrics_service_http.yaml:./$(PROTO_GEN_GO_DIR) opencensus/proto/agent/metrics/v1/metrics_service.proto
    mv $(PROTO_GEN_GO_DIR)/github.com/census-instrumentation/opencensus-proto/gen-go/* $(PROTO_GEN_GO_DIR)
    rm -fr $(PROTO_GEN_GO_DIR)/github.com

# Generate gRPC/Protobuf implementation for Go.
.PHONY: gen-python
gen-python:
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/agent/common/v1/common.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/resource/v1/resource.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/metrics/v1/metrics.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/stats/v1/stats.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/trace/v1/trace.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/trace/v1/trace_config.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/agent/common/v1/common.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) --grpc-python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/agent/metrics/v1/metrics_service.proto
    $(PROTOC) --python_out=./$(PROTO_GEN_PYTHON_DIR) --grpc-python_out=./$(PROTO_GEN_PYTHON_DIR) opencensus/proto/agent/trace/v1/trace_service.proto

# Generate gRPC/Protobuf implementation for Go.
.PHONY: gen-ruby
gen-ruby:
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/agent/common/v1/common.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/resource/v1/resource.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/metrics/v1/metrics.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/stats/v1/stats.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/trace/v1/trace.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/trace/v1/trace_config.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/agent/common/v1/common.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) --grpc-ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/agent/metrics/v1/metrics_service.proto
    $(PROTOC) --ruby_out=./$(PROTO_GEN_RUBY_DIR) --grpc-ruby_out=./$(PROTO_GEN_RUBY_DIR) opencensus/proto/agent/trace/v1/trace_service.proto

# Generate OpenApi (Swagger) documentation file for grpc-gateway endpoints.
.PHONY: gen-openapi
gen-openapi:
    $(PROTOC) --openapiv2_out=logtostderr=true,grpc_api_configuration=src/opencensus/proto/agent/trace/v1/trace_service_http.yaml:$(PROTO_GEN_OPENAPI_DIR) opencensus/proto/agent/trace/v1/trace_service.proto
    $(PROTOC) --openapiv2_out=logtostderr=true,grpc_api_configuration=src/opencensus/proto/agent/metrics/v1/metrics_service_http.yaml:$(PROTO_GEN_OPENAPI_DIR) opencensus/proto/agent/metrics/v1/metrics_service.proto