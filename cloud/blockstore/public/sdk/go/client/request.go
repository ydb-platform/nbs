package client

import (
	"fmt"
	"math/rand"
	"strings"

	protos "a.yandex-team.ru/cloud/blockstore/public/api/protos"
)

////////////////////////////////////////////////////////////////////////////////

type request interface {
	GetHeaders() *protos.THeaders
	String() string
}

////////////////////////////////////////////////////////////////////////////////

func nextRequestId() uint64 {
	var requestId uint64 = 0
	for requestId == 0 {
		requestId = rand.Uint64()
	}
	return requestId
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
	if readReq, ok := req.(*protos.TReadBlocksRequest); ok {
		return readReq.GetBlocksCount()
	}

	if writeReq, ok := req.(*protos.TWriteBlocksRequest); ok {
		return uint32(len(writeReq.Blocks.Buffers))
	}

	if zeroReq, ok := req.(*protos.TZeroBlocksRequest); ok {
		return zeroReq.GetBlocksCount()
	}

	return 0
}

func requestDetails(req request) string {
	if readReq, ok := req.(*protos.TReadBlocksRequest); ok {
		return fmt.Sprintf(
			" (offset: %d, count: %d)",
			readReq.StartIndex,
			readReq.BlocksCount)
	}

	if writeReq, ok := req.(*protos.TWriteBlocksRequest); ok {
		return fmt.Sprintf(
			" (offset: %d, count: %d)",
			writeReq.StartIndex,
			len(writeReq.Blocks.Buffers))
	}

	if zeroReq, ok := req.(*protos.TZeroBlocksRequest); ok {
		return fmt.Sprintf(
			" (offset: %d, count: %d)",
			zeroReq.StartIndex,
			zeroReq.BlocksCount)
	}

	return ""
}
