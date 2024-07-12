package test

import (
    "context"
    "io"
)

type wrappedReader struct {
    io.Reader
}

func (r *wrappedReader) ReadContext(ctx context.Context, b []byte) (int, error) {
    return r.Reader.Read(b)
}

type wrappedWriter struct {
    io.Writer
}

func (r *wrappedWriter) WriteContext(ctx context.Context, b []byte) (int, error) {
    return r.Writer.Write(b)
}

type wrappedReadWriter struct {
    io.ReadWriter
}

func (r *wrappedReadWriter) ReadContext(ctx context.Context, b []byte) (int, error) {
    return r.ReadWriter.Read(b)
}

func (r *wrappedReadWriter) WriteContext(ctx context.Context, b []byte) (int, error) {
    return r.ReadWriter.Write(b)
}
