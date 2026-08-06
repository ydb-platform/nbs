package util

import (
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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

// Keep in sync with "internal/pkg/types/types.proto : enum DiskKind"
func GetAllDiskKinds() []types.DiskKind {
	return []types.DiskKind{
		types.DiskKind_DISK_KIND_SSD,
		types.DiskKind_DISK_KIND_HDD,
		types.DiskKind_DISK_KIND_SSD_NONREPLICATED,
		types.DiskKind_DISK_KIND_SSD_MIRROR2,
		types.DiskKind_DISK_KIND_SSD_LOCAL,
		types.DiskKind_DISK_KIND_SSD_MIRROR3,
		types.DiskKind_DISK_KIND_HDD_NONREPLICATED,
		types.DiskKind_DISK_KIND_HDD_LOCAL,
	}
}
