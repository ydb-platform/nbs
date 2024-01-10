package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/coreos/go-systemd/journal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type journaldEntry struct {
	message          string
	priority         journal.Priority
	idempotencyKey   string
	requestID        string
	operationID      string
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

	idempotencyKey, ok := entry["IDEMPOTENCY_KEY"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No IDEMPOTENCY_KEY field")
	}

	requestID, ok := entry["REQUEST_ID"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No IDEMPOTENCY_KEY field")
	}

	operationID, ok := entry["OPERATION_ID"]
	if !ok {
		return journaldEntry{}, fmt.Errorf("No OPERATION_ID field")
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
		idempotencyKey:   idempotencyKey,
		requestID:        requestID,
		operationID:      operationID,
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

func runJournalctl() ([]byte, error) {
	// We want our messages to show up in journalctl, wait for them to sync.
	// journalctl --sync does not work here, because it requires special
	// access rights which are not expected on a developer's machine.
	<-time.After(time.Second)

	pid := os.Getpid()
	return exec.Command(
		"journalctl",
		"-o",
		"json",
		fmt.Sprintf("_PID=%v", pid),
		"--no-pager",
	).Output()
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

func TestJournaldLogInfo(t *testing.T) {
	logger := logging.NewJournaldLogger(logging.InfoLevel)
	ctx := logging.SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logging.Trace(ctx, "Trace did not happen")
	logging.Debug(ctx, "Debug did not happen")
	logging.Info(ctx, "Info happened")
	logging.Warn(ctx, "Warn happened")
	logging.Error(ctx, "Error happened")
	logging.Fatal(ctx, "Fatal happened")

	data, err := runJournalctl()
	require.NoError(t, err)

	entries, err := parseJournaldEntries(data)
	require.NoError(t, err)

	pc, callerFile, lineNo, ok := runtime.Caller(0)
	require.True(t, ok)

	frame, _ := runtime.CallersFrames([]uintptr{pc}).Next()
	caller := path.Base(callerFile)

	assert.EqualValues(
		t,
		[]journaldEntry{
			{
				message:          fmt.Sprintf("%s:%d Info happened", caller, lineNo-11),
				priority:         journal.PriInfo,
				idempotencyKey:   "idempID",
				requestID:        "reqID",
				operationID:      "opID",
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 11),
				codeFunc:         frame.Function,
			},
			{
				message:          fmt.Sprintf("%s:%d Warn happened", caller, lineNo-10),
				priority:         journal.PriWarning,
				idempotencyKey:   "idempID",
				requestID:        "reqID",
				operationID:      "opID",
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 10),
				codeFunc:         frame.Function,
			},
			{
				message:          fmt.Sprintf("%s:%d Error happened", caller, lineNo-9),
				priority:         journal.PriErr,
				idempotencyKey:   "idempID",
				requestID:        "reqID",
				operationID:      "opID",
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 9),
				codeFunc:         frame.Function,
			},
			{
				message:          fmt.Sprintf("%s:%d Fatal happened", caller, lineNo-8),
				priority:         journal.PriCrit,
				idempotencyKey:   "idempID",
				requestID:        "reqID",
				operationID:      "opID",
				syslogIdentifier: "disk-manager",
				codeFile:         callerFile,
				codeLine:         strconv.Itoa(lineNo - 8),
				codeFunc:         frame.Function,
			},
		},
		entries,
	)
}
