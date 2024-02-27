package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-systemd/journal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type journaldEntry struct {
	message          string
	priority         journal.Priority
	syslogIdentifier string
	codeFile         string
	codeLine         string
	codeFunc         string
}

func parseJournaldEntry(data []byte) (journaldEntry, error) {
	entry := make(map[string]string)
	err := json.Unmarshal(data, &entry)
	if err != nil {
		return journaldEntry{}, err
	}

	message, ok := entry["MESSAGE"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No MESSAGE field")
	}

	priorityString, ok := entry["PRIORITY"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No PRIORITY field")
	}
	priority, err := strconv.Atoi(priorityString)
	if err != nil {
		return journaldEntry{}, fmt.Errorf("PRIORITY field is not an int %w", err)
	}

	syslogIdentifier, ok := entry["SYSLOG_IDENTIFIER"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No SYSLOG_IDENTIFIER field")
	}

	codeFile, ok := entry["CODE_FILE"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No CODE_FILE field")
	}

	codeLine, ok := entry["CODE_LINE"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No CODE_LINE field")
	}

	codeFunc, ok := entry["CODE_FUNC"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No CODE_FUNC field")
	}

	return journaldEntry{
		message:          message,
		priority:         journal.Priority(priority),
		syslogIdentifier: syslogIdentifier,
		codeFile:         codeFile,
		codeLine:         codeLine,
		codeFunc:         codeFunc,
	}, nil
}

func parseJournaldEntries(data []byte) ([]journaldEntry, error) {
	lines := bytes.Split(data, []byte("\n"))
	entries := make([]journaldEntry, 0, len(lines))

	for i, line := range lines {
		if len(line) == 0 {
			continue
		}

		entry, err := parseJournaldEntry(line)
		if err != nil {
			return nil, fmt.Errorf(
				"Error parsing line #%v (%v): %w",
				i+1,
				string(line),
				err,
			)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

type journaldReader struct {
	readCount int
}

func (r *journaldReader) readEntries(
	t *testing.T,
	expectedNewEntriesCount int,
) (entries []journaldEntry) {
	// We want our messages to show up in journalctl, wait for them to sync.
	// journalctl --sync does not work here, because it requires special
	// access rights which are not expected on a developer's machine.
	<-time.After(time.Second)

	pid := os.Getpid()
	data, err := exec.Command(
		"journalctl",
		"-o",
		"json",
		fmt.Sprintf("_PID=%v", pid),
		"--no-pager",
	).Output()
	require.NoError(t, err)

	entries, err = parseJournaldEntries(data)
	require.NoError(t, err)

	actualNewEntriesCount := len(entries) - r.readCount
	require.EqualValues(t, expectedNewEntriesCount, actualNewEntriesCount)

	entries = entries[r.readCount:]
	r.readCount += actualNewEntriesCount
	return
}

////////////////////////////////////////////////////////////////////////////////

func newContext(
	idempotencyKey string,
	requestID string,
	operationID string,
) context.Context {

	ctx := context.Background()
	ctx = headers.SetIncomingOperationID(ctx, operationID)
	ctx = headers.SetIncomingIdempotencyKey(ctx, idempotencyKey)
	return headers.SetIncomingRequestID(ctx, requestID)
}

////////////////////////////////////////////////////////////////////////////////

func testJournaldLog(t *testing.T, reader *journaldReader) {
	logger := logging.NewJournaldLogger(logging.InfoLevel)
	ctx := logging.SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logging.Trace(ctx, "did not happen")
	logging.Debug(ctx, "did not happen")
	logging.Info(ctx, "happened")
	logging.Warn(ctx, "happened")
	logging.Error(ctx, "happened")
	logging.Fatal(ctx, "happened")

	entries := reader.readEntries(t, 4)

	pc, callerFile, lineNo, ok := runtime.Caller(0)
	require.True(t, ok)

	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	caller := path.Base(callerFile)

	fields := "IDEMPOTENCY_KEY: idempID, REQUEST_ID: reqID, OPERATION_ID: opID"

	assert.EqualValues(
		t,
		[]journaldEntry{
			{
				message:          fmt.Sprintf("%s:%d INFO => %s => happened", caller, lineNo-7, fields),
				priority:         journal.PriInfo,
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 7),
				codeFunc:         frame.Function,
			},
			{
				message:          fmt.Sprintf("%s:%d WARN => %s => happened", caller, lineNo-6, fields),
				priority:         journal.PriWarning,
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 6),
				codeFunc:         frame.Function,
			},
			{
				message:          fmt.Sprintf("%s:%d ERROR => %s => happened", caller, lineNo-5, fields),
				priority:         journal.PriErr,
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 5),
				codeFunc:         frame.Function,
			},
			{
				message:          fmt.Sprintf("%s:%d FATAL => %s => happened", caller, lineNo-4, fields),
				priority:         journal.PriCrit,
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 4),
				codeFunc:         frame.Function,
			},
		},
		entries,
	)
}

func testJournaldLogWithAdditionalFields(t *testing.T, reader *journaldReader) {
	logger := logging.NewJournaldLogger(logging.InfoLevel)
	ctx := logging.SetLogger(newContext("idempID", "reqID", "opID"), logger)

	ctx = logging.WithFields(ctx, logging.NewComponentField("component"), logging.Int("some_field", 713))

	logging.Info(ctx, "happened one")

	err := errors.New("some error")
	ctx = logging.WithFields(ctx, logging.String("another_field", "value"), logging.ErrorValue(err))

	logging.Info(ctx, "happened two")

	// Should log the latest value for duplicating keys.
	ctx = logging.WithFields(ctx, logging.Int("some_field", 714))

	logging.Info(ctx, "happened three")

	entries := reader.readEntries(t, 3)

	var messageSuffixes []string
	for _, entry := range entries {
		// Consider the suffix after the first occurance of "=>",
		// i.e. the fields and the message body.
		idx := strings.Index(entry.message, "=>") + 3
		messageSuffixes = append(messageSuffixes, entry.message[idx:])
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
	reader := &journaldReader{readCount: 0}
	testJournaldLog(t, reader)
	testJournaldLogWithAdditionalFields(t, reader)
}
