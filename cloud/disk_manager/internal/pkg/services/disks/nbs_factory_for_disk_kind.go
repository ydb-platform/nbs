package disks

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func nbsFactoryForDiskKind(
	nbsFactory nbs.Factory,
	ssdDirectMirror3Of5GroupNbsFactory nbs.Factory,
	kind types.DiskKind,
) (nbs.Factory, error) {

	if !common.IsSsdDirectMirror3Of5GroupDiskKind(kind) {
		return nbsFactory, nil
	}

	if ssdDirectMirror3Of5GroupNbsFactory == nil {
		return nil, errors.NewNonRetriableErrorf(
			"nbs config for ssd-direct-mirror3of5-group disks is not set",
		)
	}

	return ssdDirectMirror3Of5GroupNbsFactory, nil
}

func nbsFactoryForDiskKindString(
	nbsFactory nbs.Factory,
	ssdDirectMirror3Of5GroupNbsFactory nbs.Factory,
	kind string,
) (nbs.Factory, error) {

	diskKind, err := common.DiskKindFromString(kind)
	if err != nil {
		return nbsFactory, nil
	}

	return nbsFactoryForDiskKind(
		nbsFactory,
		ssdDirectMirror3Of5GroupNbsFactory,
		diskKind,
	)
}
