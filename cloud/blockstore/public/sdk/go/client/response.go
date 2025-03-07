package client

import (
	"strings"

	"github.com/ydb-platform/nbs/cloud/storage/core/protos"
)

////////////////////////////////////////////////////////////////////////////////

type response interface {
	GetError() *protos.TError
}

////////////////////////////////////////////////////////////////////////////////

func responseName(resp response) string {
	name := underlyingTypeName(resp)
	if strings.HasPrefix(name, "T") && strings.HasSuffix(name, "Response") {
		// strip "T" prefix and "Response" suffix from type name
		name = name[1 : len(name)-8]
	}
	return name
}
