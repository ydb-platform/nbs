package exporter

import (
	"context"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// A 1 GiB frame with 4 MiB blocks needs at most 256 block headers and a
// frame header/trailer beyond its raw length. Leave a conservative margin.
const s3FrameOverhead = 64 * 1024

type s3FrameBuffer struct {
	ctx   context.Context
	data  []byte
	limit int
}

func (b *s3FrameBuffer) Write(data []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}

	if len(data) > b.limit-len(b.data) ||
		len(data) > cap(b.data)-len(b.data) {

		return 0, fmt.Errorf("LZ4 frame exceeds output capacity %v", b.limit)
	}

	// Capacity is allocated once, so append can never grow the allocation.
	b.data = append(b.data, data...)
	return len(data), nil
}

type s3PartEncoder struct {
	buffer s3FrameBuffer
	writer *lz4.Writer
}

func newS3PartEncoder(partSize uint64) (*s3PartEncoder, error) {
	if partSize == 0 || partSize > MaxS3PartSize {
		return nil, fmt.Errorf("invalid LZ4 buffer size %v", partSize)
	}

	encoder := &s3PartEncoder{
		buffer: s3FrameBuffer{
			data: make([]byte, 0, int(partSize)+s3FrameOverhead),
		},
	}

	encoder.writer = lz4.NewWriter(&encoder.buffer)
	if err := encoder.writer.Apply(
		lz4.CompressionLevelOption(lz4.Fast),
		lz4.BlockSizeOption(lz4.Block4Mb),
		lz4.ConcurrencyOption(1),
		lz4.ChecksumOption(true),
	); err != nil {
		return nil, err
	}

	return encoder, nil
}

func (e *s3PartEncoder) encode(
	ctx context.Context,
	raw []byte,
) ([]byte, error) {

	limit := len(raw) + s3FrameOverhead
	if limit > cap(e.buffer.data) {
		return nil, fmt.Errorf("raw part exceeds LZ4 buffer capacity")
	}

	e.buffer.ctx = ctx
	e.buffer.data = e.buffer.data[:0]
	e.buffer.limit = limit
	e.writer.Reset(&e.buffer)
	if err := writeS3Frame(ctx, e.writer, raw); err != nil {
		return nil, err
	}

	return e.buffer.data, nil
}

func writeS3Frame(
	ctx context.Context,
	writer io.WriteCloser,
	raw []byte,
) (err error) {

	defer func() {
		closeErr := writer.Close()
		if err == nil {
			err = closeErr
		}
	}()

	for len(raw) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		length := len(raw)
		if length > chunkSize {
			length = chunkSize
		}

		n, err := writer.Write(raw[:length])
		if err != nil {
			return err
		}

		if n != length {
			return io.ErrShortWrite
		}

		raw = raw[length:]
	}

	return ctx.Err()
}
