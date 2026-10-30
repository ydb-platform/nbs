package exporter

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

const s3ProgressLogInterval = 5 * time.Second

type s3ExportProgressState struct {
	read              uint64
	uploaded          uint64
	completed         uint64
	part              uint64
	stage             string
	activeUploads     int
	activeCompression int
	activeReads       int
	assemblingParts   int
	readyParts        int
	completedParts    uint32
}

type s3ExportProgress struct {
	mu      sync.Mutex
	state   s3ExportProgressState
	metrics *s3ExportMetrics
}

func (p *s3ExportProgress) startPart(number uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if number > p.state.part {
		p.state.part = number
	}

	p.state.assemblingParts++
	p.metrics.addActive(s3StageAssemble, 1)
}

func (p *s3ExportProgress) finishPart() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.assemblingParts--
	p.metrics.addActive(s3StageAssemble, -1)
}

func (p *s3ExportProgress) addReadyPart() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.readyParts++
	p.metrics.addActive(s3StageUploadWait, 1)
}

func (p *s3ExportProgress) removeReadyPart() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.readyParts--
	p.metrics.addActive(s3StageUploadWait, -1)
}

func (p *s3ExportProgress) startRead() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.activeReads++
	p.metrics.addActive(s3StageRead, 1)
}

func (p *s3ExportProgress) finishRead() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.activeReads--
	p.metrics.addActive(s3StageRead, -1)
}

func (p *s3ExportProgress) setStage(stage string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.stage = stage
}

func (p *s3ExportProgress) addRead(size uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.read += size
	p.metrics.addRead(size)
}

func (p *s3ExportProgress) addCompleted(logical uint64, stored uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.uploaded += stored
	p.state.completed += logical
	p.state.completedParts++
	p.metrics.addUploaded(logical, stored)
}

func (p *s3ExportProgress) restoreCompleted(logical, stored uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.uploaded += stored
	p.state.completed += logical
	p.state.completedParts++
	p.metrics.addReused(logical)
}

func s3CompressionRatio(logical uint64, stored uint64) float64 {
	if logical == 0 {
		return 0
	}

	return float64(stored) / float64(logical)
}

func (p *s3ExportProgress) startCompression() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.activeCompression++
	p.metrics.addActive(s3StageCompress, 1)
}

func (p *s3ExportProgress) finishCompression() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.activeCompression--
	p.metrics.addActive(s3StageCompress, -1)
}

func (p *s3ExportProgress) startUpload() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.activeUploads++
	p.metrics.addActive(s3StageUpload, 1)
}

func (p *s3ExportProgress) finishUpload() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state.activeUploads--
	p.metrics.addActive(s3StageUpload, -1)
}

func (p *s3ExportProgress) startLogging(
	ctx context.Context,
	uri string,
	size uint64,
	partCount uint32,
	started time.Time,
) func() {

	p.mu.Lock()
	initialUploaded := p.state.uploaded
	p.mu.Unlock()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(s3ProgressLogInterval)
		defer ticker.Stop()

		var previousRead uint64
		previousTime := started
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.mu.Lock()
				state := p.state
				p.mu.Unlock()
				now := time.Now()
				stage := state.stage
				switch {
				case state.assemblingParts > 0:
					stage = "reading"
				case state.readyParts > 0:
					stage = "waiting for upload slot"
				case state.activeCompression > 0:
					stage = "compressing"
				case state.activeUploads > 0:
					stage = "waiting for uploads"
				}

				logging.Info(
					ctx,
					"backup %v: completed %v/%v logical bytes, "+
						"stored %v bytes, read %v/%v logical bytes, "+
						"last started part %v/%v, stage %v, "+
						"active reads %v, assembling parts %v, ready parts %v, "+
						"active compression workers %v, active uploads %v, "+
						"completed parts %v/%v, read speed %.2f MiB/s, "+
						"average stored speed %.2f MiB/s, ratio %.3f",
					uri,
					state.completed,
					size,
					state.uploaded,
					state.read,
					size,
					state.part,
					partCount,
					stage,
					state.activeReads,
					state.assemblingParts,
					state.readyParts,
					state.activeCompression,
					state.activeUploads,
					state.completedParts,
					partCount,
					copySpeedMiBPerSecond(
						state.read-previousRead,
						now.Sub(previousTime),
					),
					copySpeedMiBPerSecond(state.uploaded-initialUploaded, now.Sub(started)),
					s3CompressionRatio(state.completed, state.uploaded),
				)

				previousRead = state.read
				previousTime = now
			}
		}
	}()

	return func() {
		close(stop)
		<-done
	}
}
