// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bigtiff

import (
    "fmt"
    "io"

    "github.com/google/tiff"
)

const (
    MagicBigEndian        = "MM\x00\x2B"
    MagicLitEndian        = "II\x2B\x00"
    Version        uint16 = 0x2B
    VersionName    string = "BigTIFF"
)

type BigTIFF struct {
    ordr       [2]byte
    vers       uint16
    offsetSize uint16
    firstOff   uint64
    ifds       []tiff.IFD
    r          tiff.BReader
}

func (t *BigTIFF) Order() string {
    return string(t.ordr[:])
}

func (t *BigTIFF) Version() uint16 {
    return t.vers
}

func (t *BigTIFF) OffsetSize() uint16 {
    return t.offsetSize
}

func (t *BigTIFF) FirstOffset() uint64 {
    return t.firstOff
}

func (t *BigTIFF) IFDs() []tiff.IFD {
    return t.ifds
}

func (t *BigTIFF) R() tiff.BReader {
    return t.r
}

func ParseBigTIFF(ordr [2]byte, vers uint16, br tiff.BReader, tsp tiff.TagSpace, ftsp tiff.FieldTypeSpace) (out tiff.TIFF, err error) {
    if tsp == nil {
        tsp = tiff.DefaultTagSpace
    }
    if ftsp == nil {
        ftsp = tiff.DefaultFieldTypeSpace
    }

    var restOfHdr [12]byte

    if _, err = io.ReadFull(br, restOfHdr[:]); err != nil {
        return nil, fmt.Errorf("bigtiff: unable to read the rest of the header: %v", err)
    }

    offsetSize := br.ByteOrder().Uint16(restOfHdr[:2])
    if offsetSize > 8 {
        return nil, fmt.Errorf("bigtiff: unsupported offset size %d", offsetSize)
    }

    // Skip restOfHdr[2:4] since it is a constant that is normally always 0x0000 and has no use yet.

    firstOffset := br.ByteOrder().Uint64(restOfHdr[4:])
    // Check the offset to the first IFD (ensure it is past the end of the header)
    if firstOffset < 16 {
        return nil, fmt.Errorf("bigtiff: invalid offset to first IFD, %d < 16", firstOffset)
    }

    t := &BigTIFF{ordr: ordr, vers: vers, offsetSize: offsetSize, firstOff: firstOffset, r: br}

    // Locate and decode IFDs
    for nextOffset := firstOffset; nextOffset != 0; {
        var ifd tiff.IFD
        if ifd, err = ParseIFD(br, nextOffset, tsp, ftsp); err != nil {
            return nil, err
        }
        t.ifds = append(t.ifds, ifd)
        nextOffset = ifd.NextOffset()
    }
    return t, nil
}

func init() {
    tiff.RegisterVersion(Version, ParseBigTIFF)
}
