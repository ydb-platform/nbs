package tasks

import (
	"hash/crc32"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
)

// TODO:_ move this file to another package?
// TODO:_ and should we move config somewhere?

// TODO:_ do we need separate struct for this?
type tasksTracingSampler struct {
	config *tasks_config.SamplingConfigForTracing
}

func (s *tasksTracingSampler) ShouldSample(
	taskID string,
	generationID uint64, // TODO:_ generation id -- ok name?
) bool {
	if generationID > *s.config.HardBarrier { // TODO:_ Get...?
		return false
	}
	if generationID > *s.config.SoftBarrier {
		hash := crc32.ChecksumIEEE([]byte(taskID + string(generationID))) // TODO:_ make normal string, not string of one rune?
		return hash%100 < *s.config.SoftPercentage
	}
	return true
}
