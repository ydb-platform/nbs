package util

import (
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"google.golang.org/protobuf/encoding/prototext"
)

////////////////////////////////////////////////////////////////////////////////

func ParseProto(
	filepath string,
	message proto.Message,
) error {

	bytes, err := os.ReadFile(filepath)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"failed to read file %v: %w",
			filepath,
			err,
		)
	}

	messageInterface := proto.MessageV2(message)
	unmarshaller := prototext.UnmarshalOptions{DiscardUnknown: true}
	err = unmarshaller.Unmarshal(bytes, messageInterface)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"failed to parse file %v as protobuf: %w",
			filepath,
			err,
		)
	}

	return nil
}
