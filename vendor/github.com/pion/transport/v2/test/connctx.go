// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package test

import (
    "context"
    "io"
)

type wrappedReader struct {
    io.Reader
}

func (r *wrappedReader) ReadContext(_ context.Context, b []byte) (int, error) {
    return r.Reader.Read(b)
}

type wrappedWriter struct {
    io.Writer
}

func (r *wrappedWriter) WriteContext(_ context.Context, b []byte) (int, error) {
    return r.Writer.Write(b)
}

type wrappedReadWriter struct {
    io.ReadWriter
}

func (r *wrappedReadWriter) ReadContext(_ context.Context, b []byte) (int, error) {
    return r.ReadWriter.Read(b)
}

func (r *wrappedReadWriter) WriteContext(_ context.Context, b []byte) (int, error) {
    return r.ReadWriter.Write(b)
}
