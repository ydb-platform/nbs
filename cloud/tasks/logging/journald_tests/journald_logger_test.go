package tests

import (
	"errors"
	"fmt"
	"path"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/coreos/go-systemd/journal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/logging/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

func testJournaldLog(t *testing.T, reader *testcommon.JournaldReader) {
	logger := logging.NewJournaldLogger(logging.InfoLevel)
	ctx := logging.SetLogger(
		testcommon.NewContext("idempID", "reqID", "opID"),
		logger,
	)

	logging.Trace(ctx, "did not happen")
	logging.Debug(ctx, "did not happen")
	logging.Info(ctx, "happened")
	logging.Warn(ctx, "happened")
	logging.Error(ctx, "happened")
	logging.Fatal(ctx, "happened")

	entries := reader.CheckEntries(t, 4)

	pc, callerFile, lineNo, ok := runtime.Caller(0)
	require.True(t, ok)

	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	caller := path.Base(callerFile)

	fields := "IDEMPOTENCY_KEY: idempID, REQUEST_ID: reqID, OPERATION_ID: opID"

	assert.EqualValues(
		t,
		[]testcommon.JournaldEntry{
			{
				Message:          fmt.Sprintf("%s:%d INFO => %s => happened", caller, lineNo-7, fields),
				Priority:         journal.PriInfo,
				IdempotencyKey:   "idempID",
				RequestID:        "reqID",
				OperationID:      "opID",
				SyslogIdentifier: "disk-manager",
				CodeFile:         callerFile,
				CodeLine:         strconv.Itoa(lineNo - 7),
				CodeFunc:         frame.Function,
			},
			{
				Message:          fmt.Sprintf("%s:%d WARN => %s => happened", caller, lineNo-6, fields),
				Priority:         journal.PriWarning,
				IdempotencyKey:   "idempID",
				RequestID:        "reqID",
				OperationID:      "opID",
				SyslogIdentifier: "disk-manager",
				CodeFile:         callerFile,
				CodeLine:         strconv.Itoa(lineNo - 6),
				CodeFunc:         frame.Function,
			},
			{
				Message:          fmt.Sprintf("%s:%d ERROR => %s => happened", caller, lineNo-5, fields),
				Priority:         journal.PriErr,
				IdempotencyKey:   "idempID",
				RequestID:        "reqID",
				OperationID:      "opID",
				SyslogIdentifier: "disk-manager",
				CodeFile:         callerFile,
				CodeLine:         strconv.Itoa(lineNo - 5),
				CodeFunc:         frame.Function,
			},
			{
				Message:          fmt.Sprintf("%s:%d FATAL => %s => happened", caller, lineNo-4, fields),
				Priority:         journal.PriCrit,
				IdempotencyKey:   "idempID",
				RequestID:        "reqID",
				OperationID:      "opID",
				SyslogIdentifier: "disk-manager",
				CodeFile:         callerFile,
				CodeLine:         strconv.Itoa(lineNo - 4),
				CodeFunc:         frame.Function,
			},
		},
		entries,
	)
}

func testJournaldLogWithAdditionalFields(
	t *testing.T,
	reader *testcommon.JournaldReader,
) {

	logger := logging.NewJournaldLogger(logging.InfoLevel)
	ctx := logging.SetLogger(
		testcommon.NewContext("idempID", "reqID", "opID"),
		logger,
	)

	ctx = logging.WithFields(ctx, logging.NewComponentField("component"), logging.Int("some_field", 713))

	logging.Info(ctx, "happened one")

	err := errors.New("some error")
	ctx = logging.WithFields(ctx, logging.String("another_field", "value"), logging.ErrorValue(err))

	logging.Info(ctx, "happened two")

	// Should log the latest value for duplicating keys.
	ctx = logging.WithFields(ctx, logging.Int("some_field", 714))

	logging.Info(ctx, "happened three")

	entries := reader.CheckEntries(t, 3)

	var messageSuffixes []string
	for _, entry := range entries {
		// Consider the suffix after the first occurance of "=>",
		// i.e. the fields and the message body.
		idx := strings.Index(entry.Message, "=>") + 3
		messageSuffixes = append(messageSuffixes, entry.Message[idx:])
	}

	expectedSuffixes := []string{
		"IDEMPOTENCY_KEY: idempID, REQUEST_ID: reqID, OPERATION_ID: opID, COMPONENT: component, SOME_FIELD: 713 => happened one",
		"IDEMPOTENCY_KEY: idempID, REQUEST_ID: reqID, OPERATION_ID: opID, COMPONENT: component, SOME_FIELD: 713, ANOTHER_FIELD: value => happened two => some error",
		"IDEMPOTENCY_KEY: idempID, REQUEST_ID: reqID, OPERATION_ID: opID, COMPONENT: component, SOME_FIELD: 714, ANOTHER_FIELD: value => happened three => some error",
	}

	assert.EqualValues(
		t,
		expectedSuffixes,
		messageSuffixes,
	)
}

func TestJournaldLog(t *testing.T) {
	reader := &testcommon.JournaldReader{ReadCount: 0}
	testJournaldLog(t, reader)
	testJournaldLogWithAdditionalFields(t, reader)
}
