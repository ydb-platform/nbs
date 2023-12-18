package client

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////
// We should combine errors from different facilities:
// * Generic errors
// * System errors (file system, network etc)
// * gRPC errors
// * KiKiMR errors
// * Application errors (storage, service etc)

//nolint:st1003
const (
	SEVERITY_SUCCESS = iota
	SEVERITY_ERROR
)

//nolint:st1003
const (
	FACILITY_NULL = iota
	FACILITY_SYSTEM
	FACILITY_GRPC
	FACILITY_KIKIMR
	FACILITY_SCHEMESHARD
	FACILITY_BLOCKSTORE
	FACILITY_TXPROXY
	FACILITY_FILESTORE
)

func succeeded(code uint32) bool {
	return (code & 0x80000000) == 0
}

func failed(code uint32) bool {
	return (code & 0x80000000) != 0
}

func facilityFromCode(code uint32) uint32 {
	return (code & 0x7FFFFFFF) >> 16
}

func statusFromCode(code uint32) uint32 {
	return (code & 0x0000FFFF)
}

func makeResultCode(severity uint32, facility uint32, status uint32) uint32 {
	return ((severity & 0x00000001) << 31) | ((facility & 0x00007FFF) << 16) | (status & 0x0000FFFF)
}

func makeSuccess(status uint32) uint32 {
	return makeResultCode(SEVERITY_SUCCESS, FACILITY_NULL, status)
}

func makeError(status uint32) uint32 {
	return makeResultCode(SEVERITY_ERROR, FACILITY_NULL, status)
}

func makeSystemError(status uint32) uint32 {
	return makeResultCode(SEVERITY_ERROR, FACILITY_SYSTEM, status)
}

func makeGrpcError(status uint32) uint32 {
	return makeResultCode(SEVERITY_ERROR, FACILITY_GRPC, status)
}

func makeKikimrError(status uint32) uint32 {
	return makeResultCode(SEVERITY_ERROR, FACILITY_KIKIMR, status)
}

func makeSchemeShardError(status uint32) uint32 {
	return makeResultCode(SEVERITY_ERROR, FACILITY_SCHEMESHARD, status)
}

func makeTxProxyError(status uint32) uint32 {
	return makeResultCode(SEVERITY_ERROR, FACILITY_TXPROXY, status)
}

func makeFileStoreError(status uint32) uint32 {
	return makeResultCode(SEVERITY_ERROR, FACILITY_FILESTORE, status)
}

////////////////////////////////////////////////////////////////////////////////
// Well-known result codes

//nolint:st1003
var (
	S_OK      = makeSuccess(0)
	S_FALSE   = makeSuccess(1)
	S_ALREADY = makeSuccess(2)

	E_FAIL            = makeError(0)
	E_ARGUMENT        = makeError(1)
	E_REJECTED        = makeError(2)
	E_IO              = makeError(3)
	E_INVALID_STATE   = makeError(4)
	E_TIMEOUT         = makeError(5)
	E_NOT_FOUND       = makeError(6)
	E_UNAUTHORIZED    = makeError(7)
	E_NOT_IMPLEMENTED = makeError(8)
	E_ABORTED         = makeError(9)

	E_GRPC_CANCELLED           = makeGrpcError(1)
	E_GRPC_UNKNOWN             = makeGrpcError(2)
	E_GRPC_INVALID_ARGUMENT    = makeGrpcError(3)
	E_GRPC_DEADLINE_EXCEEDED   = makeGrpcError(4)
	E_GRPC_NOT_FOUND           = makeGrpcError(5)
	E_GRPC_ALREADY_EXISTS      = makeGrpcError(6)
	E_GRPC_PERMISSION_DENIED   = makeGrpcError(7)
	E_GRPC_RESOURCE_EXHAUSTED  = makeGrpcError(8)
	E_GRPC_FAILED_PRECONDITION = makeGrpcError(9)
	E_GRPC_ABORTED             = makeGrpcError(10)
	E_GRPC_OUT_OF_RANGE        = makeGrpcError(11)
	E_GRPC_UNIMPLEMENTED       = makeGrpcError(12)
	E_GRPC_INTERNAL            = makeGrpcError(13)
	E_GRPC_UNAVAILABLE         = makeGrpcError(14)
	E_GRPC_DATA_LOSS           = makeGrpcError(15)
	E_GRPC_UNAUTHENTICATED     = makeGrpcError(16)

	E_FS_INVALID_SESSION = makeFileStoreError(100)
	E_FS_OUT_OF_SPACE    = makeFileStoreError(101)
)

////////////////////////////////////////////////////////////////////////////////

var facilityMap = map[uint32]string{
	FACILITY_NULL:        "FACILITY_NULL",
	FACILITY_SYSTEM:      "FACILITY_SYSTEM",
	FACILITY_GRPC:        "FACILITY_GRPC",
	FACILITY_KIKIMR:      "FACILITY_KIKIMR",
	FACILITY_SCHEMESHARD: "FACILITY_SCHEMESHARD",
	FACILITY_BLOCKSTORE:  "FACILITY_BLOCKSTORE",
	FACILITY_TXPROXY:     "FACILITY_TXPROXY",
	FACILITY_FILESTORE:   "FACILITY_FILESTORE",
}

var resultMap = map[uint32]string{
	S_OK:      "S_OK",
	S_FALSE:   "S_FALSE",
	S_ALREADY: "S_ALREADY",

	E_FAIL:            "E_FAIL",
	E_ARGUMENT:        "E_ARGUMENT",
	E_REJECTED:        "E_REJECTED",
	E_IO:              "E_IO",
	E_INVALID_STATE:   "E_INVALID_STATE",
	E_TIMEOUT:         "E_TIMEOUT",
	E_NOT_FOUND:       "E_NOT_FOUND",
	E_UNAUTHORIZED:    "E_UNAUTHORIZED",
	E_NOT_IMPLEMENTED: "E_NOT_IMPLEMENTED",

	E_GRPC_CANCELLED:           "E_GRPC_CANCELLED",
	E_GRPC_UNKNOWN:             "E_GRPC_UNKNOWN",
	E_GRPC_INVALID_ARGUMENT:    "E_GRPC_INVALID_ARGUMENT",
	E_GRPC_DEADLINE_EXCEEDED:   "E_GRPC_DEADLINE_EXCEEDED",
	E_GRPC_NOT_FOUND:           "E_GRPC_NOT_FOUND",
	E_GRPC_ALREADY_EXISTS:      "E_GRPC_ALREADY_EXISTS",
	E_GRPC_PERMISSION_DENIED:   "E_GRPC_PERMISSION_DENIED",
	E_GRPC_RESOURCE_EXHAUSTED:  "E_GRPC_RESOURCE_EXHAUSTED",
	E_GRPC_FAILED_PRECONDITION: "E_GRPC_FAILED_PRECONDITION",
	E_GRPC_ABORTED:             "E_GRPC_ABORTED",
	E_GRPC_OUT_OF_RANGE:        "E_GRPC_OUT_OF_RANGE",
	E_GRPC_UNIMPLEMENTED:       "E_GRPC_UNIMPLEMENTED",
	E_GRPC_INTERNAL:            "E_GRPC_INTERNAL",
	E_GRPC_UNAVAILABLE:         "E_GRPC_UNAVAILABLE",
	E_GRPC_DATA_LOSS:           "E_GRPC_DATA_LOSS",
	E_GRPC_UNAUTHENTICATED:     "E_GRPC_UNAUTHENTICATED",

	E_FS_INVALID_SESSION: "E_FS_INVALID_SESSION",
	E_FS_OUT_OF_SPACE:    "E_FS_OUT_OF_SPACE",
}

func getSeverityString(code uint32) string {
	if succeeded(code) {
		return "SEVERITY_SUCCESS"
	} else {
		return "SEVERITY_ERROR"
	}
}

func getFacilityString(code uint32) string {
	if str, ok := facilityMap[facilityFromCode(code)]; ok {
		return str
	}
	return "FACILITY_UNKNOWN"
}

func formatErrorCodeRaw(code uint32) string {
	return fmt.Sprintf(
		"%s | %s | %d",
		getSeverityString(code),
		getFacilityString(code),
		statusFromCode(code))
}

func formatErrorCode(code uint32) string {
	if str, ok := resultMap[code]; ok {
		return str
	}
	return formatErrorCodeRaw(code)
}
