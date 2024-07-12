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

// These constants represents the first 4 bytes of the file for each kind of
// TIFF along with each byte ordering.  This is mostly useful for registration
// with the "image" package from the Go standard library.
const (
    MagicBigEndian        = "MM\x00\x2A"
    MagicLitEndian        = "II\x2A\x00"
    Version        uint16 = 0x2A
    VersionName    string = "TIFF"
)

// These constants represent the byte order options present at the beginning of
// a TIFF file.
const (
    BigEndian uint16 = 0x4D4D // "MM" or 19789
    LitEndian uint16 = 0x4949 // "II" or 18761
)

func GetByteOrder(bo uint16) binary.ByteOrder {
    switch bo {
    case BigEndian:
        return binary.BigEndian
    case LitEndian:
        return binary.LittleEndian
    }
    return nil
}

type ErrInvalidByteOrder struct {
    Order [2]byte
}

func (e ErrInvalidByteOrder) Error() string {
    return fmt.Sprintf("tiff: invalid byte order %q", e.Order)
}

type ErrUnsuppTIFFVersion struct {
    Version uint16
}

func (e ErrUnsuppTIFFVersion) Error() string {
    return fmt.Sprintf("tiff: unsupported version %d", e.Version)
}

type Header interface {
    Order() string
    Version() uint16
    OffsetSize() uint16
    FirstOffset() uint64
}

type TIFFParser func(ordr [2]byte, vers uint16, br BReader, tsp TagSpace, ftsp FieldTypeSpace) (TIFF, error)

type TIFF interface {
    Header
    IFDs() []IFD
    R() BReader
}

func Parse(r ReadAtReadSeeker, tsp TagSpace, ftsp FieldTypeSpace) (TIFF, error) {
    if tsp == nil {
        tsp = DefaultTagSpace
    }
    if ftsp == nil {
        ftsp = DefaultFieldTypeSpace
    }

    var magicBytes [4]byte

    if _, err := io.ReadFull(r, magicBytes[:]); err != nil {
        return nil, fmt.Errorf("tiff: unable to read byte order and version: %v", err)
    }

    orderBytes := [2]byte{magicBytes[0], magicBytes[1]}
    byteOrder := GetByteOrder(binary.BigEndian.Uint16(orderBytes[:]))
    if byteOrder == nil {
        return nil, ErrInvalidByteOrder{orderBytes}
    }

    vers := byteOrder.Uint16(magicBytes[2:])

    tp := GetVersionParser(vers)
    if tp == nil {
        return nil, ErrUnsuppTIFFVersion{vers}
    }
    return tp(orderBytes, vers, NewBReader(r, byteOrder), tsp, ftsp)
}

// Type tiff represents a standard tiff structure with 32 bit offsets.
type tiff struct {
    ordr     [2]byte
    vers     uint16
    firstOff uint32
    ifds     []IFD
    r        BReader
}

func (t *tiff) Order() string {
    return string(t.ordr[:])
}

func (t *tiff) Version() uint16 {
    return t.vers
}

func (t *tiff) OffsetSize() uint16 {
    return 4
}

func (t *tiff) FirstOffset() uint64 {
    return uint64(t.firstOff)
}

func (t *tiff) IFDs() []IFD {
    return t.ifds
}

func (t *tiff) R() BReader {
    return t.r
}

func ParseTIFF(ordr [2]byte, vers uint16, br BReader, tsp TagSpace, ftsp FieldTypeSpace) (out TIFF, err error) {
    if tsp == nil {
        tsp = DefaultTagSpace
    }
    if ftsp == nil {
        ftsp = DefaultFieldTypeSpace
    }

    var firstOffset uint32
    // Get the offset to the first IFD
    if err = br.BRead(&firstOffset); err != nil {
        return nil, fmt.Errorf("tiff: unable to read offset to first ifd: %v", err)
    }
    // Check the offset to the first IFD (ensure it is past the end of the header)
    if firstOffset < 8 {
        return nil, fmt.Errorf("tiff: invalid offset to first IFD, %d < 8", firstOffset)
    }

    t := &tiff{ordr: ordr, vers: vers, firstOff: firstOffset, r: br}
    // Locate and decode IFDs
    for nextOffset := uint64(firstOffset); nextOffset != 0; {
        var ifd IFD
        if ifd, err = ParseIFD(br, nextOffset, tsp, ftsp); err != nil {
            return
        }
        t.ifds = append(t.ifds, ifd)
        nextOffset = ifd.NextOffset()
    }
    return t, nil
}

var versionParsers = struct {
    mu      sync.RWMutex
    parsers map[uint16]TIFFParser
}{
    parsers: make(map[uint16]TIFFParser, 1),
}

func RegisterVersion(v uint16, tp TIFFParser) {
    versionParsers.mu.Lock()
    defer versionParsers.mu.Unlock()
    versionParsers.parsers[v] = tp
}

func GetVersionParser(v uint16) TIFFParser {
    versionParsers.mu.RLock()
    defer versionParsers.mu.RUnlock()
    return versionParsers.parsers[v]
}

func init() {
    RegisterVersion(Version, ParseTIFF)
}
