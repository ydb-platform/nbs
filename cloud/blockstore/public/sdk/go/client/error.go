package client

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type ClientError struct {
	Code    uint32 `json:"code"`
	Message string `json:"message"`
}

func (e *ClientError) Succeeded() bool {
	return succeeded(e.Code)
}

func (e *ClientError) Failed() bool {
	return failed(e.Code)
}

func (e *ClientError) Facility() uint32 {
	return facilityFromCode(e.Code)
}

func (e *ClientError) Status() uint32 {
	return statusFromCode(e.Code)
}

func (e *ClientError) Error() string {
	if e.Message != "" {
		return formatErrorCode(e.Code) + " " + e.Message
	} else {
		return formatErrorCode(e.Code)
	}
}

func (e *ClientError) IsRetriable() bool {
	switch e.Code {
	case E_REJECTED, E_TIMEOUT, E_THROTTLED, E_TRY_AGAIN, E_OUT_OF_SPACE:
		// special error code for retries
		return true
	}

	switch e.Facility() {
	case FACILITY_GRPC, FACILITY_SYSTEM:
		// system/network errors should be retriable
		return true
	case FACILITY_KIKIMR:
		switch e.Status() {
		case
			1,  // NKikimrProto::ERROR
			3,  // NKikimrProto::TIMEOUT
			4,  // NKikimrProto::RACE
			6,  // NKikimrProto::BLOCKED
			7,  // NKikimrProto::NOTREADY
			12, // NKikimrProto::DEADLINE
			20: // NKikimrProto::NOT_YET
			return true
		}
	}

	// any other errors should not be retried automatically
	return false
}

////////////////////////////////////////////////////////////////////////////////

func NewClientError(err error) *ClientError {
	if err == nil {
		return nil
	}

	if status, ok := status.FromError(err); ok {
		if status.Code() == codes.OK {
			return &ClientError{S_OK, status.Message()}
		}

		return &ClientError{
			Code:    makeGrpcError(uint32(status.Code())),
			Message: status.Message(),
		}
	}

	return &ClientError{E_FAIL, err.Error()}
}

func GetClientCode(err error) uint32 {
	if err == nil {
		return S_OK
	}

	if cerr, ok := err.(*ClientError); ok {
		return cerr.Code
	}

	return E_FAIL
}

func GetClientError(err error) ClientError {
	if err == nil {
		return ClientError{S_OK, ""}
	}

	if cerr, ok := err.(*ClientError); ok {
		return *cerr
	}

	return ClientError{E_FAIL, err.Error()}
}
