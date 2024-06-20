package testcommon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/coreos/go-systemd/journal"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

func NewContext(
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

type JournaldEntry struct {
	Message          string
	Priority         journal.Priority
	IdempotencyKey   string
	RequestID        string
	OperationID      string
	SyslogIdentifier string
	CodeFile         string
	CodeLine         string
	CodeFunc         string
}

func parseJournaldEntry(data []byte) (JournaldEntry, error) {
	entry := make(map[string]string)
	err := json.Unmarshal(data, &entry)
	if err != nil {
		return JournaldEntry{}, err
	}

	message, ok := entry["MESSAGE"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No MESSAGE field")
	}

	priorityString, ok := entry["PRIORITY"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No PRIORITY field")
	}
	priority, err := strconv.Atoi(priorityString)
	if err != nil {
		return JournaldEntry{}, fmt.Errorf("PRIORITY field is not an int %w", err)
	}

	idempotencyKey, ok := entry["IDEMPOTENCY_KEY"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No IDEMPOTENCY_KEY field")
	}

	requestID, ok := entry["REQUEST_ID"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No IDEMPOTENCY_KEY field")
	}

	operationID, ok := entry["OPERATION_ID"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No OPERATION_ID field")
	}

	syslogIdentifier, ok := entry["SYSLOG_IDENTIFIER"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No SYSLOG_IDENTIFIER field")
	}

	codeFile, ok := entry["CODE_FILE"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No CODE_FILE field")
	}

	codeLine, ok := entry["CODE_LINE"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No CODE_LINE field")
	}

	codeFunc, ok := entry["CODE_FUNC"]
	if !ok {
		return JournaldEntry{}, fmt.Errorf("No CODE_FUNC field")
	}

	return JournaldEntry{
		Message:          message,
		Priority:         journal.Priority(priority),
		IdempotencyKey:   idempotencyKey,
		RequestID:        requestID,
		OperationID:      operationID,
		SyslogIdentifier: syslogIdentifier,
		CodeFile:         codeFile,
		CodeLine:         codeLine,
		CodeFunc:         codeFunc,
	}, nil
}

func parseJournaldEntries(data []byte) ([]JournaldEntry, error) {
	lines := bytes.Split(data, []byte("\n"))
	entries := make([]JournaldEntry, 0, len(lines))

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

////////////////////////////////////////////////////////////////////////////////

type JournaldReader struct {
	ReadCount int
}

func (r *JournaldReader) ReadEntries(
	t *testing.T,
) []JournaldEntry {

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

	entries, err := parseJournaldEntries(data)
	require.NoError(t, err)

	return entries
}

func (r *JournaldReader) CheckEntries(
	t *testing.T,
	expectedNewEntriesCount int,
) []JournaldEntry {

	entries := r.ReadEntries(t)

	actualNewEntriesCount := len(entries) - r.ReadCount
	require.EqualValues(t, expectedNewEntriesCount, actualNewEntriesCount)

	entries = entries[r.ReadCount:]
	r.ReadCount += actualNewEntriesCount

	return entries
}
