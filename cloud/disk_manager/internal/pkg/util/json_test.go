package util

import (
	"encoding/json"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

func TestExportTaskStateJSONOmitsCheckpoint(t *testing.T) {
	state := &dataplane_protos.ExportSnapshotTaskState{
		Source:            "primary",
		ExportState:       []byte(`{"completed":false}`),
		Progress:          0.5,
		UploadWorkerLimit: 2,
	}
	data, err := proto.Marshal(state)
	require.NoError(t, err)

	for _, taskType := range []string{
		"dataplane.ExportSnapshotToS3", "dataplane.ExportImageToS3",
	} {
		t.Run(taskType, func(t *testing.T) {
			dump := TaskStateToJSON(&storage.TaskState{
				TaskType: taskType,
				State:    data,
			})
			encoded, err := dump.Marshal()
			require.NoError(t, err)
			var decoded struct {
				State json.RawMessage `json:"state"`
			}
			require.NoError(t, json.Unmarshal(encoded, &decoded))
			require.JSONEq(t,
				`{"Source":"primary","Progress":0.5,"UploadWorkerLimit":2}`,
				string(decoded.State),
			)

			// Formatting must preserve the complete underlying protobuf state.
			retained, err := proto.Marshal(dump.State)
			require.NoError(t, err)
			require.Equal(t, data, retained)
		})
	}
}
