package client

import (
	"math/rand"
	"strings"

	protos "a.yandex-team.ru/cloud/filestore/public/api/protos"
)

////////////////////////////////////////////////////////////////////////////////

type request interface {
	GetHeaders() *protos.THeaders
	String() string
}

////////////////////////////////////////////////////////////////////////////////

func nextRequestID() uint64 {
	var requestID uint64 = 0
	for requestID == 0 {
		requestID = rand.Uint64()
	}
	return requestID
}

func requestName(req request) string {
	name := underlyingTypeName(req)
	if strings.HasPrefix(name, "T") && strings.HasSuffix(name, "Request") {
		// strip "T" prefix and "Request" suffix from type name
		name = name[1 : len(name)-7]
	}
	return name
}

func requestSize(req request) uint32 {
	// TODO
	return 0
}

func requestDetails(req request) string {
	// TODO
	return ""
}
