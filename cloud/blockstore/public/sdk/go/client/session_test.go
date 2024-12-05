package client

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

const (
	DefaultDiskId      = "path/to/test_volume"
	DefaultMountToken  = "mountToken"
	DefaultBlockSize   = 4 * 1024
	DefaultBlocksCount = 1024
)

func createSession(client *testClient, makeTicker tickerFactory) *Session {
	if makeTicker == nil {
		makeTicker = func(period time.Duration) ticker {
			return newTimeTicker(period)
		}
	}

	return &Session{
		client:     Client{safeClient{client}},
		log:        NewStderrLog(LOG_DEBUG),
		makeTicker: makeTicker,
		volume:     new(protos.TVolume),
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestReturnErrorWhenSessionIsClosed(t *testing.T) {
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			response := &protos.TMountVolumeResponse{
				SessionId: "1",
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			t.Errorf("Closed session should not trigger automatic unmount")
			return &protos.TUnmountVolumeResponse{}, nil
		},
		ReadBlocksHandler: func(
			ctx context.Context,
			req *protos.TReadBlocksRequest,
		) (*protos.TReadBlocksResponse, error) {
			t.Errorf("Session is closed. Should not pass ReadBlocks request to client")
			return &protos.TReadBlocksResponse{}, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			t.Errorf("Session is closed. Should not pass WriteBlocks request to client")
			return &protos.TWriteBlocksResponse{}, nil
		},
		ZeroBlocksHandler: func(
			ctx context.Context,
			req *protos.TZeroBlocksRequest,
		) (*protos.TZeroBlocksResponse, error) {
			t.Errorf("Session is closed. Should not pass ZeroBlocks request to client")
			return &protos.TZeroBlocksResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	session.Close()

	_, err = session.ReadBlocks(ctx, 0, 1, "")
	if err == nil {
		t.Errorf("Session is closed. Should return error for ReadBlocks")
	}

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, 4096)
		buffers = append(buffers, buffer)
	}

	err = session.WriteBlocks(ctx, 0, buffers)
	if err == nil {
		t.Errorf("Session is closed. Should return error for WriteBlocks")
	}

	err = session.ZeroBlocks(ctx, 0, 1)
	if err == nil {
		t.Errorf("Session is closed. Should return error for ZeroBlocks")
	}
}

func TestMountVolume(t *testing.T) {
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			response := &protos.TMountVolumeResponse{
				SessionId: "1",
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "1" {
				t.Errorf("Wrong session id (expected: 1, actual: %s)",
					req.SessionId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}
}

func readWriteBlocksTestCommon(blockSize uint32) error {
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			return &protos.TMountVolumeResponse{
				SessionId: "1",
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   blockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}, nil

		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "1" {
				return nil, fmt.Errorf("Wrong session id (expected: 1, actual: %s)",
					req.SessionId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
		ReadBlocksHandler: func(
			ctx context.Context,
			req *protos.TReadBlocksRequest,
		) (*protos.TReadBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "1" {
				return nil, fmt.Errorf("Wrong session id (expected: 1, actual: %s)",
					req.SessionId)
			}

			// simulate zero response
			buffers := make([][]byte, 0, req.BlocksCount)
			for i := 0; i < int(req.BlocksCount); i++ {
				buffer := make([]byte, blockSize)
				buffers = append(buffers, buffer)
			}

			return &protos.TReadBlocksResponse{
				Blocks: &protos.TIOVector{
					Buffers: buffers,
				},
			}, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "1" {
				return nil, fmt.Errorf("Wrong session id (expected: 1, actual: %s)",
					req.SessionId)
			}

			return &protos.TWriteBlocksResponse{}, nil
		},
		ZeroBlocksHandler: func(
			ctx context.Context,
			req *protos.TZeroBlocksRequest,
		) (*protos.TZeroBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "1" {
				return nil, fmt.Errorf("Wrong session id (expected: 1, actual: %s)",
					req.SessionId)
			}

			return &protos.TZeroBlocksResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		return err
	}

	_, err = session.ReadBlocks(ctx, 0, 1, "")
	if err != nil {
		return err
	}

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, blockSize)
		buffers = append(buffers, buffer)
	}

	err = session.WriteBlocks(ctx, 0, buffers)
	if err != nil {
		return err
	}

	err = session.ZeroBlocks(ctx, 0, 1)
	if err != nil {
		return err
	}

	err = session.UnmountVolume(ctx)
	if err != nil {
		return err
	}

	return nil
}

func TestReadWriteBlocks(t *testing.T) {
	err := readWriteBlocksTestCommon(DefaultBlockSize)
	if err != nil {
		t.Error(err)
	}
}

func TestReadWriteNonDefaultBlockSize(t *testing.T) {
	var blockSize uint32 = 512
	if DefaultBlockSize == int(blockSize) {
		// fix me if default block size changes
		t.Errorf("DefaultBlockSize is %d", blockSize)
	}
	err := readWriteBlocksTestCommon(blockSize)
	if err != nil {
		t.Error(err)
	}
}

func TestRemountVolumeOnInvalidSession(t *testing.T) {
	sessionId := 0
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			sessionId++
			response := &protos.TMountVolumeResponse{
				SessionId: strconv.Itoa(sessionId),
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "2" {
				t.Errorf("Wrong session id (expected: 2, actual: %s)",
					req.SessionId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId == "1" {
				return nil, &ClientError{
					Code: E_INVALID_SESSION,
				}
			}

			return &protos.TWriteBlocksResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 1 {
		t.Errorf("Wrong session id (expected: 1, actual: %d)",
			sessionId)
	}

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, DefaultBlockSize)
		buffers = append(buffers, buffer)
	}

	err = session.WriteBlocks(ctx, 0, buffers)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 2 {
		t.Errorf("Wrong session id (expected: 2, actual: %d)",
			sessionId)
	}

	err = session.UnmountVolume(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestRemountPeriodicallyToPreventUnmountDueToInactivity(t *testing.T) {
	timeout := 100 * time.Millisecond
	sessionId := 0
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			sessionId++
			response := &protos.TMountVolumeResponse{
				SessionId:              strconv.Itoa(sessionId),
				InactiveClientsTimeout: uint32(durationToMsec(timeout)),
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
	}

	fakeTicker := newFakeTicker()
	fakeTickerFactory := newFakeTickerFactory(fakeTicker)

	session := createSession(client, fakeTickerFactory)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 1 {
		t.Errorf("Wrong session id (expected: 1, actual: %d)",
			sessionId)
	}

	fakeTicker.SendTick()

	// Send tick again to ensure remount causes scheduling of another remount
	fakeTicker.SendTick()

	err = session.UnmountVolume(ctx)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 3 {
		t.Errorf("Wrong session id (expected: 3, actual: %d)",
			sessionId)
	}
}

func TestStopAutomaticRemountAfterUnmount(t *testing.T) {
	timeout := 1 * time.Second
	sessionId := 0
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			sessionId++
			response := &protos.TMountVolumeResponse{
				SessionId:              strconv.Itoa(sessionId),
				InactiveClientsTimeout: uint32(durationToMsec(timeout)),
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
	}

	fakeTicker := newFakeTicker()
	fakeTickerFactory := newFakeTickerFactory(fakeTicker)

	session := createSession(client, fakeTickerFactory)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 1 {
		t.Errorf("Wrong session id (expected: 1, actual: %d)",
			sessionId)
	}

	fakeTicker.SendTick()

	if sessionId <= 1 {
		t.Errorf("Wrong session id (expected: >1, actual: %d)",
			sessionId)
	}

	if fakeTicker.Stopped {
		t.Error("Ticker is stopped before the volume is unmounted")
	}

	err = session.UnmountVolume(ctx)
	if err != nil {
		t.Error(err)
	}

	if !fakeTicker.Stopped {
		t.Error("Remount ticker is not stopped after volume unmounting")
	}
}

func TestCorrectlyReportErrorsAfterRemount(t *testing.T) {
	sessionId := 0
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			sessionId++
			response := &protos.TMountVolumeResponse{
				SessionId: strconv.Itoa(sessionId),
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "2" {
				t.Errorf("Wrong session id (expected: 2, actual: %s)",
					req.SessionId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId == "1" {
				return nil, &ClientError{
					Code: E_INVALID_SESSION,
				}
			}

			return nil, &ClientError{
				Code: E_ARGUMENT,
			}
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 1 {
		t.Errorf("Wrong session id (expected: 1, actual: %d)",
			sessionId)
	}

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, DefaultBlockSize)
		buffers = append(buffers, buffer)
	}

	err = session.WriteBlocks(ctx, 0, buffers)
	if err == nil {
		t.Errorf("Unexpectedly missing error in response to WriteBlocks")
	}

	cerr, ok := err.(*ClientError)
	if !ok {
		t.Errorf("Failed to convert error in response to WriteBlocks to ClientError: %v", err)
	}

	if cerr.Code != E_ARGUMENT {
		t.Errorf("Unexpected error in response to WriteBlocks: (%d) %s",
			cerr.Code, cerr.Message)
	}

	err = session.UnmountVolume(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestRejectReadWriteZeroBlocksRequestsForUnmountedVolume(t *testing.T) {
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			response := &protos.TMountVolumeResponse{
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
		ReadBlocksHandler: func(
			ctx context.Context,
			req *protos.TReadBlocksRequest,
		) (*protos.TReadBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			// simulate zero response
			buffers := make([][]byte, 0, req.BlocksCount)
			for i := 0; i < int(req.BlocksCount); i++ {
				buffer := make([]byte, DefaultBlockSize)
				buffers = append(buffers, buffer)
			}

			return &protos.TReadBlocksResponse{
				Blocks: &protos.TIOVector{
					Buffers: buffers,
				},
			}, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			return &protos.TWriteBlocksResponse{}, nil
		},
		ZeroBlocksHandler: func(
			ctx context.Context,
			req *protos.TZeroBlocksRequest,
		) (*protos.TZeroBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			return &protos.TZeroBlocksResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, DefaultBlockSize)
		buffers = append(buffers, buffer)
	}

	err := session.WriteBlocks(ctx, 0, buffers)
	if err == nil {
		t.Errorf("Unexpectedly missing error on WriteBlocks for unmounted volume")
	}

	cerr, ok := err.(*ClientError)
	if !ok {
		t.Errorf("Failed to convert WriteBlocks error to ClientError: %v", err)
	}

	if cerr.Code != E_INVALID_STATE {
		t.Errorf("Unexpected error on WriteBlocks for unmounted volume: (%d) %s",
			cerr.Code, cerr.Message)
	}

	err = session.ZeroBlocks(ctx, 0, 1)
	if err == nil {
		t.Errorf("Unexpectedly missing error on ZeroBlocks for unmounted volume")
	}

	cerr, ok = err.(*ClientError)
	if !ok {
		t.Errorf("Failed to convert ZeroBlocks error to ClientError: %v", err)
	}

	if cerr.Code != E_INVALID_STATE {
		t.Errorf("Unexpected error on ZeroBlocks for unmounted volume: (%d) %s",
			cerr.Code, cerr.Message)
	}

	_, err = session.ReadBlocks(ctx, 0, 1, "")
	if err == nil {
		t.Errorf("Unexpectedly missing error on ReadBlocks for unmounted volume")
	}

	cerr, ok = err.(*ClientError)
	if !ok {
		t.Errorf("Failed to convert ReadBlocks error to ClientError: %v", err)
	}

	if cerr.Code != E_INVALID_STATE {
		t.Errorf("Unexpected error on ReadBlocks for unmounted volume: (%d) %s",
			cerr.Code, cerr.Message)
	}
}

func TestShouldTryRemountOnWriteFollowingPreviousRemountError(t *testing.T) {
	timeout := 100 * time.Millisecond
	sessionId := 0
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			sessionId++

			if sessionId == 2 {
				return nil, &ClientError{
					Code: E_INVALID_STATE,
				}
			}

			response := &protos.TMountVolumeResponse{
				SessionId:              strconv.Itoa(sessionId),
				InactiveClientsTimeout: uint32(durationToMsec(timeout)),
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			return &protos.TWriteBlocksResponse{}, nil
		},
	}

	fakeTicker := newFakeTicker()
	fakeTickerFactory := newFakeTickerFactory(fakeTicker)

	session := createSession(client, fakeTickerFactory)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 1 {
		t.Errorf("Wrong session id (expected: 1, actual: %d)",
			sessionId)
	}

	fakeTicker.SendTick()

	// One remount attempt should have been done
	if sessionId != 2 {
		t.Errorf("Wrong session id (expected: 2, actual: %d)",
			sessionId)
	}

	if !fakeTicker.Stopped {
		t.Error("Ticker is not stopped after unsuccessful remount")
	}

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, DefaultBlockSize)
		buffers = append(buffers, buffer)
	}

	// The next write blocks attempt should cause one more remount and then pass
	err = session.WriteBlocks(ctx, 0, buffers)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 3 {
		t.Errorf("Wrong session id (expected: 3, actual: %d)",
			sessionId)
	}

	if fakeTicker.Stopped {
		t.Error("Ticker was not restarted after successful remount")
	}
}

func TestMultipleInFlightReadWriteZeroBlocksRequests(t *testing.T) {
	var inFlightReadCounter int32
	var inFlightWriteCounter int32
	var inFlightZeroCounter int32

	var maxInFlightReadCounter int32
	var maxInFlightWriteCounter int32
	var maxInFlightZeroCounter int32

	var readCounterLock sync.Mutex
	var writeCounterLock sync.Mutex
	var zeroCounterLock sync.Mutex

	var completeRequestMutex sync.Mutex
	completeRequestCond := sync.NewCond(&completeRequestMutex)
	completeRequestFlag := false

	var numConcurrentRequests int32 = 20

	var wgEntered sync.WaitGroup
	wgEntered.Add(int(numConcurrentRequests) * 3)

	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			response := &protos.TMountVolumeResponse{
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
		ReadBlocksHandler: func(
			ctx context.Context,
			req *protos.TReadBlocksRequest,
		) (*protos.TReadBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			readCounterLock.Lock()
			inFlightReadCounter++
			if maxInFlightReadCounter < inFlightReadCounter {
				maxInFlightReadCounter = inFlightReadCounter
			}
			readCounterLock.Unlock()

			wgEntered.Done()

			completeRequestCond.L.Lock()
			for !completeRequestFlag {
				completeRequestCond.Wait()
			}
			defer completeRequestCond.L.Unlock()

			// simulate zero response
			buffers := make([][]byte, 0, req.BlocksCount)
			for i := 0; i < int(req.BlocksCount); i++ {
				buffer := make([]byte, DefaultBlockSize)
				buffers = append(buffers, buffer)
			}

			atomic.AddInt32(&inFlightReadCounter, -1)

			return &protos.TReadBlocksResponse{
				Blocks: &protos.TIOVector{
					Buffers: buffers,
				},
			}, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			writeCounterLock.Lock()
			inFlightWriteCounter++
			if maxInFlightWriteCounter < inFlightWriteCounter {
				maxInFlightWriteCounter = inFlightWriteCounter
			}
			writeCounterLock.Unlock()

			wgEntered.Done()

			completeRequestCond.L.Lock()
			for !completeRequestFlag {
				completeRequestCond.Wait()
			}
			defer completeRequestCond.L.Unlock()

			atomic.AddInt32(&inFlightWriteCounter, -1)

			return &protos.TWriteBlocksResponse{}, nil
		},
		ZeroBlocksHandler: func(
			ctx context.Context,
			req *protos.TZeroBlocksRequest,
		) (*protos.TZeroBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			zeroCounterLock.Lock()
			inFlightZeroCounter++
			if maxInFlightZeroCounter < inFlightZeroCounter {
				maxInFlightZeroCounter = inFlightZeroCounter
			}
			zeroCounterLock.Unlock()

			wgEntered.Done()

			completeRequestCond.L.Lock()
			for !completeRequestFlag {
				completeRequestCond.Wait()
			}
			defer completeRequestCond.L.Unlock()

			atomic.AddInt32(&inFlightZeroCounter, -1)

			return &protos.TZeroBlocksResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, DefaultBlockSize)
		buffers = append(buffers, buffer)
	}

	var wgDone sync.WaitGroup
	wgDone.Add(int(numConcurrentRequests) * 3)

	for i := 0; i < int(numConcurrentRequests); i++ {
		go func() {
			defer wgDone.Done()
			e := session.WriteBlocks(ctx, 0, buffers)
			if e != nil {
				t.Error(e)
			}
		}()
	}

	for i := 0; i < int(numConcurrentRequests); i++ {
		go func() {
			defer wgDone.Done()
			e := session.ZeroBlocks(ctx, 0, 1)
			if e != nil {
				t.Error(e)
			}
		}()
	}

	for i := 0; i < int(numConcurrentRequests); i++ {
		go func() {
			defer wgDone.Done()
			_, e := session.ReadBlocks(ctx, 0, 1, "")
			if e != nil {
				t.Error(e)
			}
		}()
	}

	wgEntered.Wait()

	completeRequestCond.L.Lock()
	completeRequestFlag = true
	completeRequestCond.L.Unlock()
	completeRequestCond.Broadcast()

	wgDone.Wait()

	if maxInFlightWriteCounter != numConcurrentRequests {
		t.Errorf("Wrong max in-flight write blocks requests (expected: %d, actual: %d)",
			numConcurrentRequests, maxInFlightWriteCounter)
	}

	if maxInFlightZeroCounter != numConcurrentRequests {
		t.Errorf("Wrong max in-flight zero blocks requests (expected: %d, actual: %d)",
			numConcurrentRequests, maxInFlightZeroCounter)
	}

	if maxInFlightReadCounter != numConcurrentRequests {
		t.Errorf("Wrong max in-flight read blocks requests (expected: %d, actual: %d)",
			numConcurrentRequests, maxInFlightReadCounter)
	}
}

func TestUseOriginalMountRequestHeadersForRemounts(t *testing.T) {
	sessionId := 0
	idempotenceId := "test_idempotence_id"
	traceId := "test_trace_id"
	clientId := "test_client_id"
	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			ctxIdempotenceId := ctx.Value(IdempotenceIdHeaderKey).(string)
			if ctxIdempotenceId != idempotenceId {
				t.Errorf("Wrong idempotence id (expected: %s, actual: %s)",
					idempotenceId, ctxIdempotenceId)
			}

			ctxTraceId := ctx.Value(TraceIdHeaderKey).(string)
			if ctxTraceId != traceId {
				t.Errorf("Wrong trace id (expected: %s, actual: %s)",
					traceId, ctxTraceId)
			}

			ctxClientId := ctx.Value(ClientIdHeaderKey).(string)
			if ctxClientId != clientId {
				t.Errorf("Wrong client id (expected: %s, actual: %s)",
					clientId, ctxClientId)
			}

			sessionId++
			response := &protos.TMountVolumeResponse{
				SessionId: strconv.Itoa(sessionId),
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "2" {
				t.Errorf("Wrong session id (expected: 2, actual: %s)",
					req.SessionId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			if req.DiskId != DefaultDiskId {
				return nil, fmt.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId == "1" {
				return nil, &ClientError{
					Code: E_INVALID_SESSION,
				}
			}

			return &protos.TWriteBlocksResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()
	ctx = context.WithValue(
		ctx,
		IdempotenceIdHeaderKey,
		idempotenceId)
	ctx = context.WithValue(
		ctx,
		TraceIdHeaderKey,
		traceId)
	ctx = context.WithValue(
		ctx,
		ClientIdHeaderKey,
		clientId)

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 1 {
		t.Errorf("Wrong session id (expected: 1, actual: %d)",
			sessionId)
	}

	buffers := make([][]byte, 0, DefaultBlocksCount)
	for i := 0; i < DefaultBlocksCount; i++ {
		buffer := make([]byte, DefaultBlockSize)
		buffers = append(buffers, buffer)
	}

	err = session.WriteBlocks(ctx, 0, buffers)
	if err != nil {
		t.Error(err)
	}

	if sessionId != 2 {
		t.Errorf("Wrong session id (expected: 2, actual: %d)",
			sessionId)
	}

	err = session.UnmountVolume(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestMountRequestSerialization(t *testing.T) {
	var inFlightMountCounter int32
	var maxInFlightMountCounter int32
	var mountCounterLock sync.Mutex
	var setupCompleted bool
	var numConcurrentRequests int32 = 20

	var completeRequestMutex sync.Mutex
	completeRequestCond := sync.NewCond(&completeRequestMutex)
	completeRequestFlag := true

	var wgEntered sync.WaitGroup
	wgEntered.Add(int(numConcurrentRequests))

	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			if setupCompleted {
				mountCounterLock.Lock()
				inFlightMountCounter++
				if maxInFlightMountCounter < inFlightMountCounter {
					maxInFlightMountCounter = inFlightMountCounter
				}
				mountCounterLock.Unlock()

				wgEntered.Done()

				completeRequestCond.L.Lock()
				for !completeRequestFlag {
					completeRequestCond.Wait()
				}
				defer completeRequestCond.L.Unlock()

				atomic.AddInt32(&inFlightMountCounter, -1)
			}

			response := &protos.TMountVolumeResponse{
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	setupCompleted = true

	var wgDone sync.WaitGroup
	wgDone.Add(int(numConcurrentRequests))

	for i := 0; i < int(numConcurrentRequests); i++ {
		go func() {
			defer wgDone.Done()
			e := session.MountVolume(ctx, DefaultDiskId, nil)
			if e != nil {
				t.Error(e)
			}
		}()
	}

	wgEntered.Wait()

	completeRequestCond.L.Lock()
	completeRequestFlag = true
	completeRequestCond.L.Unlock()
	completeRequestCond.Broadcast()

	wgDone.Wait()

	if maxInFlightMountCounter != 1 {
		t.Errorf("Concurrent mount requests detected (%d)",
			maxInFlightMountCounter)
	}
}

func TestMountUnmountRequestSerialization(t *testing.T) {
	var inFlightCounter int32
	var maxInFlightCounter int32
	var counterLock sync.Mutex
	var setupCompleted bool

	var completeRequestMutex sync.Mutex
	completeRequestCond := sync.NewCond(&completeRequestMutex)
	completeRequestFlag := true

	var wgEntered sync.WaitGroup
	wgEntered.Add(2)

	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			if setupCompleted {
				counterLock.Lock()
				inFlightCounter++
				if maxInFlightCounter < inFlightCounter {
					maxInFlightCounter = inFlightCounter
				}
				counterLock.Unlock()

				wgEntered.Done()

				completeRequestCond.L.Lock()
				for !completeRequestFlag {
					completeRequestCond.Wait()
				}
				defer completeRequestCond.L.Unlock()

				atomic.AddInt32(&inFlightCounter, -1)
			}

			response := &protos.TMountVolumeResponse{
				SessionId: "1",
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}

			if setupCompleted {
				counterLock.Lock()
				inFlightCounter++
				if maxInFlightCounter < inFlightCounter {
					maxInFlightCounter = inFlightCounter
				}
				counterLock.Unlock()

				wgEntered.Done()

				completeRequestCond.L.Lock()
				for !completeRequestFlag {
					completeRequestCond.Wait()
				}
				defer completeRequestCond.L.Unlock()

				atomic.AddInt32(&inFlightCounter, -1)
			}

			return &protos.TUnmountVolumeResponse{}, nil
		},
	}

	session := createSession(client, nil)
	ctx := context.TODO()

	err := session.MountVolume(ctx, DefaultDiskId, nil)
	if err != nil {
		t.Error(err)
	}

	setupCompleted = true

	var wgDone sync.WaitGroup
	wgDone.Add(2)

	go func() {
		defer wgDone.Done()
		e := session.MountVolume(ctx, DefaultDiskId, nil)
		if e != nil {
			t.Error(e)
		}
	}()

	go func() {
		defer wgDone.Done()
		e := session.UnmountVolume(ctx)
		if e != nil {
			t.Error(e)
		}
	}()

	wgEntered.Wait()

	completeRequestCond.L.Lock()
	completeRequestFlag = true
	completeRequestCond.L.Unlock()
	completeRequestCond.Broadcast()

	wgDone.Wait()

	if maxInFlightCounter != 1 {
		t.Errorf("Concurrent mount/unmount requests detected (%d)",
			maxInFlightCounter)
	}
}

func TestRemountsStopAfterUnmount(t *testing.T) {
	var mounted atomic.Bool

	client := &testClient{
		MountVolumeHandler: func(
			ctx context.Context,
			req *protos.TMountVolumeRequest,
		) (*protos.TMountVolumeResponse, error) {
			if !mounted.CompareAndSwap(false, true) {
				t.Errorf("Mount conflict")
			}

			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			response := &protos.TMountVolumeResponse{
				SessionId: "1",
				Volume: &protos.TVolume{
					DiskId:      req.DiskId,
					BlockSize:   DefaultBlockSize,
					BlocksCount: DefaultBlocksCount,
				},
				InactiveClientsTimeout: uint32(500), // 500 milliseconds
			}
			return response, nil
		},
		UnmountVolumeHandler: func(
			ctx context.Context,
			req *protos.TUnmountVolumeRequest,
		) (*protos.TUnmountVolumeResponse, error) {
			mounted.Store(false)

			// Waiting for a remounter tick in order to provoke race condition.
			time.Sleep(time.Second)

			if req.DiskId != DefaultDiskId {
				t.Errorf("Wrong disk id (expected: %s, actual: %s)",
					DefaultDiskId, req.DiskId)
			}
			if req.SessionId != "1" {
				t.Errorf("Wrong session id (expected: 1, actual: %s)",
					req.SessionId)
			}
			return &protos.TUnmountVolumeResponse{}, nil
		},
	}

	for i := 0; i < 2; i++ {
		session := createSession(client, nil)
		ctx := context.TODO()

		err := session.MountVolume(ctx, DefaultDiskId, nil)
		if err != nil {
			t.Error(err)
		}

		err = session.UnmountVolume(ctx)
		if err != nil {
			t.Error(err)
		}

		// Volume should not be remounted during this sleep.
		time.Sleep(time.Second)
		session.Close()
		// Volume should not be remounted during this sleep.
		time.Sleep(time.Second)
	}
}
