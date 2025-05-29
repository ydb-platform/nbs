package shards

import (
	"context"
	"slices"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	shards_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/shards/config"
)

////////////////////////////////////////////////////////////////////////////////

type service struct {
	config *shards_config.ShardsConfig
}

func NewService(
	config *shards_config.ShardsConfig,
) Service {

	return &service{
		config: config,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *service) PickShard(
	ctx context.Context,
	disk *disk_manager.DiskId,
	folderID string,
) string {

	if !s.isShardingAllowedForFolder(folderID) {
		return disk.ZoneId
	}

	shards := s.getShards(disk.ZoneId)

	if len(shards) == 0 {
		// We end up here if an unsharded zone or a shard of a zone is
		// provided as ZoneId.
		return disk.ZoneId
	}

	return shards[0]
}

////////////////////////////////////////////////////////////////////////////////

func (s *service) getShards(zoneID string) []string {
	shards, ok := s.config.Shards[zoneID]
	if !ok {
		return []string{}
	}

	return shards.Shards
}

func (s *service) isShardingAllowedForFolder(folderID string) bool {
	return (len(rule.IncludedFolders.GetFolders()) == 0 ||
		slices.Contains(rule.IncludedFolders.GetFolders(), folderID)) &&
		!slices.Contains(rule.ExcludedFolders.GetFolders(), folderID)
}
