// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

import (
    "encoding/binary"
    "fmt"
    "io"

    "sync"
)

// ReadAtReadSeeker is the interface that wraps the Read, ReadAt, and Seek
// methods.  Typical use cases would satisfy this with a bytes.Reader (in
// memory) or an os.File (on disk).  For truly large files, such as BigTIFF, a
// user may want to create a custom solution that combines both in memory and on
// disk solutions for accessing the contents.
type ReadAtReadSeeker interface {
    io.ReadSeeker
    io.ReaderAt
}

// buffer buffers an io.Reader to satisfy ReadAtReadSeeker.  Seeking from the
// end is not supported.  This should be okay since this is for internal use
// only.
type buffer struct {
    mu  sync.Mutex
    r   io.Reader
    pos int
    buf []byte
}

// fill reads data from b.r until the buffer contains at least end bytes.
func (b *buffer) fill(end int) error {
    b.mu.Lock()
    defer b.mu.Unlock()
    m := len(b.buf)
    if end > m {
        if end > cap(b.buf) {
            newcap := 6144
            for newcap < end {
                newcap *= 2
            }
            newbuf := make([]byte, end, newcap)
            copy(newbuf, b.buf)
            b.buf = newbuf
        } else {
            b.buf = b.buf[:end]
        }
        if n, err := io.ReadFull(b.r, b.buf[m:end]); err != nil {
            end = m + n
            b.buf = b.buf[:end]
            return err
        }
    }
    return nil
}

func (b *buffer) ReadAt(p []byte, off int64) (int, error) {
    o := int(off)
    end := o + len(p)
    if int64(end) != off+int64(len(p)) {
        return 0, io.ErrUnexpectedEOF
    }

    err := b.fill(end)
    return copy(p, b.buf[o:end]), err
}

func (b *buffer) Read(p []byte) (int, error) {
    end := b.pos + len(p)
    if err := b.fill(end); err != nil {
        return 0, err
    }
    n := copy(p, b.buf[b.pos:end])
    b.pos = end
    return n, nil
}

func (b *buffer) Seek(offset int64, whence int) (int64, error) {
    var newPos int
    // In this package, we only plan to support cases 0 & 1 with case 0
    // being the default and case 1 explicit option.  Case 2 would require
    // loading the entire contents into memory or trying to assert b.r as
    // an *os.File or an io.Seeker.
    switch whence {
    case 1:
        newPos = b.pos + int(offset)
    case 2:
        return 0, fmt.Errorf("tiff: seeking from the end of file is not supported")
    default:
        newPos = int(offset)
    }

    // TODO: Make sure that offset was not a value that can only be
    // expressed as an int64.  This is only of concern for 32 bit systems.

    err := b.fill(newPos)
    if newPos > len(b.buf) {
        b.pos = len(b.buf)
    } else {
        b.pos = newPos
    }
    return int64(b.pos), err
}

// Section returns b as an io.SectionReader to allow access to a specific chunk
// in the buffer.
func (b *buffer) Section(off, n int) *io.SectionReader {
    return io.NewSectionReader(b, int64(off), int64(n))
}

// NewReadAtReadSeeker converts r (an io.Reader) into a ReadAtReadSeeker.  If
// the underlying type of r can satisfy a ReadAtReadSeeker, it is asserted as
// such and used directly instead of being wrapped.
func NewReadAtReadSeeker(r io.Reader) ReadAtReadSeeker {
    if rars, ok := r.(ReadAtReadSeeker); ok {
        return rars
    }
    return &buffer{
        r:   r,
        pos: 0,
        buf: make([]byte, 0, 3072),
    }
}

// BReader wraps a ReadAtReadSeeker with a specific binary.ByteOrder.
type BReader interface {
    BRead(data interface{}) error
    BReadSection(data interface{}, offset int64, n int64) error
    ByteOrder() binary.ByteOrder
    ReadAtReadSeeker
}

func NewBReader(r ReadAtReadSeeker, o binary.ByteOrder) BReader {
    return &bReader{order: o, r: r}
}

// bReader wraps a ReadAtReadSeeker and reads it with a specific
// binary.ByteOrder.
type bReader struct {
    order binary.ByteOrder
    r     ReadAtReadSeeker
}

func (b *bReader) Read(p []byte) (n int, err error) {
    return b.r.Read(p)
}

func (b *bReader) ReadAt(p []byte, off int64) (n int, err error) {
    return b.r.ReadAt(p, off)
}

func (b *bReader) BRead(data interface{}) error {
    return binary.Read(b.r, b.order, data)
}

func (b *bReader) BReadSection(data interface{}, offset int64, n int64) error {
    if offset < 0 {
        return fmt.Errorf("tiff: invalid offset %d", offset)
    }
    if n < 1 {
        return fmt.Errorf("tiff: invalid section size %d", n)
    }
    sr := io.NewSectionReader(b.r, offset, n)
    return binary.Read(sr, b.order, data)
}

func (b *bReader) Seek(offset int64, whence int) (int64, error) {
    return b.r.Seek(offset, whence)
}

func (b *bReader) ByteOrder() binary.ByteOrder {
    return b.order
}

type uint16Slice []uint16

func (p uint16Slice) Len() int           { return len(p) }
func (p uint16Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint16Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
