package client

import (
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type mountState uint32

const (
	stateUninitialized mountState = iota
	stateMountCompleted
	stateMountRequested
)

////////////////////////////////////////////////////////////////////////////////

type remountCtx struct {
	baseContext context.Context
}

func (*remountCtx) Deadline() (deadline time.Time, ok bool) {
	return
}

func (*remountCtx) Done() <-chan struct{} {
	return nil
}

func (*remountCtx) Err() error {
	return nil
}

func (r *remountCtx) Value(key interface{}) interface{} {
	return r.baseContext.Value(key)
}

func (*remountCtx) String() string {
	return "remountCtx"
}

////////////////////////////////////////////////////////////////////////////////

type remounter struct {
	Ticker     ticker
	Done       chan bool
	remountCtx *remountCtx
}

////////////////////////////////////////////////////////////////////////////////

type Session struct {
	client Client
	log    Log

	mountLock sync.Mutex
	state     mountState
	mountOpts MountVolumeOpts
	volume    *protos.TVolume
	info      SessionInfo
	closed    bool

	mountHeaders protos.THeaders

	makeTicker tickerFactory
	remounter  *remounter
}

func (s *Session) Volume() *protos.TVolume {
	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	return proto.Clone(s.volume).(*protos.TVolume)
}

func (s *Session) Info() SessionInfo {
	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	return s.info
}

func (s *Session) MountVolume(
	ctx context.Context,
	diskId string,
	opts *MountVolumeOpts,
) error {

	s.fetchMountHeaders(ctx)

	if logger := s.log.Logger(LOG_DEBUG); logger != nil {
		logger.Printf(
			ctx,
			"Submit MountVolume for %s, client_id: %v, opts: %v",
			diskId,
			s.mountHeaders.ClientId,
			opts,
		)
	}

	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	if s.closed {
		return &ClientError{
			Code:    E_INVALID_STATE,
			Message: "Session is already closed",
		}
	}

	volume, info, err := s.client.MountVolume(ctx, diskId, opts)

	err = s.processMountResponse(ctx, diskId, volume, info, err)
	if err != nil {
		return err
	}

	if opts != nil {
		s.mountOpts = *opts
	}

	s.volume = volume
	return nil
}

func (s *Session) UnmountVolume(ctx context.Context) error {
	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	if s.closed {
		return &ClientError{
			Code:    E_INVALID_STATE,
			Message: "Session is already closed",
		}
	}

	if s.state == stateUninitialized {
		return &ClientError{
			Code:    E_INVALID_STATE,
			Message: "Volume is not mounted",
		}
	}

	if logger := s.log.Logger(LOG_DEBUG); logger != nil {
		logger.Printf(
			ctx,
			"Submit UnmountVolume for %s, client_id: %v",
			s.volume.DiskId,
			s.mountHeaders.ClientId,
		)
	}

	err := s.client.UnmountVolume(ctx, s.volume.DiskId, s.info.SessionId)
	if err != nil {
		return err
	}

	s.stopRemounter()
	s.resetState()
	return nil
}

func (s *Session) ReadBlocks(
	ctx context.Context,
	startIndex uint64,
	blocksCount uint32,
	checkpointId string,
) ([][]byte, error) {

	var res [][]byte
	err := s.processRequest(
		ctx,
		func(ctx context.Context, sessionId string, diskId string) error {
			var err error
			res, err = s.client.ReadBlocks(
				ctx,
				diskId,
				startIndex,
				blocksCount,
				checkpointId,
				sessionId)
			return err
		})

	return res, err
}

func (s *Session) WriteBlocks(
	ctx context.Context,
	startIndex uint64,
	blocks [][]byte,
) error {

	return s.processRequest(
		ctx,
		func(ctx context.Context, sessionId string, diskId string) error {
			return s.client.WriteBlocks(
				ctx,
				diskId,
				startIndex,
				blocks,
				sessionId)
		})
}

func (s *Session) ZeroBlocks(
	ctx context.Context,
	startIndex uint64,
	blocksCount uint32,
) error {

	return s.processRequest(
		ctx,
		func(ctx context.Context, sessionId string, diskId string) error {
			return s.client.ZeroBlocks(
				ctx,
				diskId,
				startIndex,
				blocksCount,
				sessionId)
		})
}

func (s *Session) Close() {
	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	s.closed = true
	s.stopRemounter()
	s.resetState()
}

func (s *Session) resetState() {
	s.mountOpts = MountVolumeOpts{}
	s.volume = &protos.TVolume{
		DiskId: s.volume.DiskId,
	}
	s.info = SessionInfo{}
	s.state = stateUninitialized
}

func (s *Session) processRequest(
	ctx context.Context,
	call func(ctx context.Context, sessionId string, diskid string) error,
) error {

	for {
		sessionId, diskId, err := s.ensureVolumeMounted(ctx)
		if err != nil {
			return err
		}

		err = call(ctx, sessionId, diskId)

		if GetClientCode(err) != E_INVALID_SESSION {
			return err
		}

		s.mountLock.Lock()
		s.state = stateMountRequested
		s.mountLock.Unlock()
	}
}

func (s *Session) ensureVolumeMounted(ctx context.Context) (string, string, error) {
	s.mountLock.Lock()
	defer s.mountLock.Unlock()

	if s.closed {
		return "", "", &ClientError{
			Code:    E_INVALID_STATE,
			Message: "Session is already closed",
		}
	}

	if s.state == stateUninitialized {
		return "", "", &ClientError{
			Code:    E_INVALID_STATE,
			Message: "Volume is not mounted",
		}
	}

	var err error
	sessionId := s.info.SessionId
	diskId := s.volume.DiskId

	if s.state == stateMountRequested {
		if logger := s.log.Logger(LOG_WARN); logger != nil {
			logger.Printf(
				ctx,
				"Force remount volume %s, client_id: %v",
				diskId,
				s.mountHeaders.ClientId,
			)
		}

		sessionId, err = s.remountVolume(ctx)
		if err != nil {
			return "", "", err
		}
	}

	return sessionId, diskId, nil
}

func (s *Session) remountVolume(ctx context.Context) (string, error) {

	ctx = s.setupMountHeaders(ctx)
	diskId := s.volume.DiskId

	if logger := s.log.Logger(LOG_DEBUG); logger != nil {
		logger.Printf(
			ctx,
			"Remount volume %s, client_id: %v",
			diskId,
			s.mountHeaders.ClientId,
		)
	}

	volume, info, err := s.client.MountVolume(
		ctx,
		diskId,
		&s.mountOpts)

	err = s.processMountResponse(ctx, diskId, volume, info, err)
	if err != nil {
		s.stopRemounter()
		s.resetState()
		s.state = stateMountRequested
		return "", err
	}

	return s.info.SessionId, nil
}

func (s *Session) fetchMountHeaders(ctx context.Context) {
	if val := ctx.Value(IdempotenceIdHeaderKey); val != nil {
		if str, ok := val.(string); ok {
			s.mountHeaders.IdempotenceId = str
		}
	}

	if val := ctx.Value(TraceIdHeaderKey); val != nil {
		if str, ok := val.(string); ok {
			s.mountHeaders.TraceId = str
		}
	}

	if val := ctx.Value(RequestTimeoutHeaderKey); val != nil {
		if d, ok := val.(time.Duration); ok {
			s.mountHeaders.RequestTimeout = uint32(durationToMsec(d))
		}
	}

	if val := ctx.Value(ClientIdHeaderKey); val != nil {
		if str, ok := val.(string); ok {
			s.mountHeaders.ClientId = str
		}
	}
}

func (s *Session) setupMountHeaders(ctx context.Context) context.Context {
	mountCtx := context.WithValue(
		ctx,
		IdempotenceIdHeaderKey,
		s.mountHeaders.IdempotenceId)

	mountCtx = context.WithValue(
		mountCtx,
		TraceIdHeaderKey,
		s.mountHeaders.TraceId)

	mountCtx = context.WithValue(
		mountCtx,
		RequestTimeoutHeaderKey,
		s.mountHeaders.RequestTimeout)

	mountCtx = context.WithValue(
		mountCtx,
		ClientIdHeaderKey,
		s.mountHeaders.ClientId)

	return mountCtx
}

func (s *Session) processMountResponse(
	ctx context.Context,
	diskId string,
	volume *protos.TVolume,
	info *SessionInfo,
	err error,
) error {

	if err == nil {
		if s.volume.BlockSize != 0 {
			// validate volume geometry
			if s.volume.BlockSize != volume.BlockSize {
				err = &ClientError{
					Code:    E_FAIL,
					Message: "Volume geometry changed",
				}
			}
		}
	}

	if err != nil {
		if logger := s.log.Logger(LOG_ERROR); logger != nil {
			logger.Printf(
				ctx,
				"MountVolume for %s failed: %v, client_id: %s",
				diskId,
				err,
				s.mountHeaders.ClientId,
			)
		}

		// we do not want to hide mount failure from client,
		// so request will NOT be automatically retried.
		// just force remount on next request
		s.state = stateMountRequested
		return err
	}

	if logger := s.log.Logger(LOG_DEBUG); logger != nil {
		logger.Printf(
			ctx,
			"Complete MountVolume for %s, client_id: %s",
			diskId,
			s.mountHeaders.ClientId,
		)
	}

	previousRemountPeriod := s.info.InactiveClientsTimeout

	s.volume = volume
	s.info = *info
	s.state = stateMountCompleted

	if s.info.InactiveClientsTimeout != previousRemountPeriod {
		s.stopRemounter()

		if s.info.InactiveClientsTimeout != 0 {
			s.startRemounter(ctx)
		}
	}

	return nil
}

func (s *Session) stopRemounter() {
	if s.remounter != nil {
		s.remounter.Done <- true
		close(s.remounter.Done)
		s.remounter.Ticker.Stop()
		s.remounter = nil
	}
}

func (s *Session) startRemounter(ctx context.Context) {
	s.remounter = &remounter{
		Ticker:     s.makeTicker(s.info.InactiveClientsTimeout),
		Done:       make(chan bool, 1),
		remountCtx: &remountCtx{ctx},
	}

	go func(r *remounter) {
		for {
			select {
			case <-r.Done:
				r.Ticker.Stop()
				return
			case <-r.Ticker.TickChan():
				s.mountLock.Lock()

				select {
				case <-r.Done:
				default:
					_, _ = s.remountVolume(r.remountCtx)
				}

				s.mountLock.Unlock()
				r.Ticker.TickProcessed()
			}
		}
	}(s.remounter)
}

////////////////////////////////////////////////////////////////////////////////

func NewSession(client Client, log Log) *Session {

	return &Session{
		client: client,
		log:    log,
		volume: new(protos.TVolume),
		makeTicker: func(period time.Duration) ticker {
			return newTimeTicker(period)
		},
	}
}
