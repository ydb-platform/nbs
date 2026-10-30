package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type unfinishedTaskStorage struct {
	Storage
	tasks []TaskState
	err   error
	calls int
}

func (s *unfinishedTaskStorage) ListUnfinishedTasks(ctx context.Context, taskType string, after string, limit uint64) ([]TaskState, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	var page []TaskState
	for _, task := range s.tasks {
		if task.ID > after && task.TaskType == taskType && !IsEnded(task.Status) {
			page = append(page, task)
			if uint64(len(page)) == limit {
				break
			}
		}
	}
	return page, nil
}

func TestCompoundListUnfinishedTasksMergesPages(t *testing.T) {
	before := time.Unix(100, 0)
	task := func(id, folder string) TaskState {
		return TaskState{ID: id, TaskType: "delete", StorageFolder: folder, CreatedAt: before.Add(-time.Second)}
	}
	legacy := &unfinishedTaskStorage{tasks: []TaskState{task("b", "legacy"), task("d", "legacy"), task("f", "legacy")}}
	current := &unfinishedTaskStorage{tasks: []TaskState{task("a", "current"), task("d", "current"), task("e", "current")}}
	storage := &compoundStorage{legacyStorage: legacy, storage: current}
	var ids []string
	cursor := ""
	for {
		page, err := storage.ListUnfinishedTasks(context.Background(), "delete", cursor, 2)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		require.LessOrEqual(t, len(page), 2)
		for _, row := range page {
			ids = append(ids, row.ID)
			if row.ID == "d" {
				require.Equal(t, "legacy", row.StorageFolder)
			}
		}
		cursor = page[len(page)-1].ID
	}
	require.Equal(t, []string{"a", "b", "d", "e", "f"}, ids)
	require.Equal(t, 4, legacy.calls)
	require.Equal(t, 4, current.calls)
}

func TestCompoundListUnfinishedTasksFailsWholePage(t *testing.T) {
	for _, failLegacy := range []bool{false, true} {
		legacy := &unfinishedTaskStorage{}
		current := &unfinishedTaskStorage{}
		if failLegacy {
			legacy.err = assert.AnError
		} else {
			current.err = assert.AnError
		}
		storage := &compoundStorage{legacyStorage: legacy, storage: current}
		page, err := storage.ListUnfinishedTasks(context.Background(), "delete", "", 100)
		require.ErrorIs(t, err, assert.AnError)
		require.Nil(t, page)
	}
}
