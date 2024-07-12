// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bigtiff

import (
    "encoding/json"
    "fmt"

    "github.com/google/tiff"
)

/*
Entry structure
  For IFD/Entry:
    Each 20-byte IFD entry has the following format:
    Bytes 0-1:   The Tag that identifies the entry.
    Bytes 2-3:   The entry Type.
    Bytes 4-11:  The number of values, Count of the indicated Type.
    Bytes 12-19: The Value Offset, the file offset (in bytes) of the Value
                 for the entry. The Value is expected to begin on a word
                 boundary; the corresponding Value Offset will thus be an
                 even number. This file offset may point anywhere in the
                 file, even after the image data.
*/

// Entry represents a single entry in an IFD in a BigTIFF file.  This is the
// mostly uninterpreted core 20 byte data structure only.
type Entry interface {
    TagID() uint16
    TypeID() uint16
    Count() uint64
    ValueOffset() [8]byte
}

// entry represents the data structure of an IFD entry.
type entry struct {
    tagID       uint16  // Bytes 0-1
    typeID      uint16  // Bytes 2-3
    count       uint64  // Bytes 4-11
    valueOffset [8]byte // Bytes 12-19
}

func (e *entry) TagID() uint16 {
    return e.tagID
}

func (e *entry) TypeID() uint16 {
    return e.typeID
}

func (e *entry) Count() uint64 {
    return e.count
}

func (e *entry) ValueOffset() [8]byte {
    return e.valueOffset
}

func (e *entry) String() string {
    return fmt.Sprintf("<TagID: %5d, TypeID: %5d, Count: %d, ValueOffset: %v>", e.tagID, e.typeID, e.count, e.valueOffset)
}

func (e *entry) MarshalJSON() ([]byte, error) {
    tmp := struct {
        Tag         uint16  `json:"tagID"`
        Type        uint16  `json:"typeID"`
        Count       uint64  `json:"count"`
        ValueOffset [8]byte `json:"valueOffset"`
    }{
        Tag:         e.tagID,
        Type:        e.typeID,
        Count:       e.count,
        ValueOffset: e.valueOffset,
    }
    return json.Marshal(tmp)
}

func ParseEntry(br tiff.BReader) (out Entry, err error) {
    e := new(entry)
    if err = br.BRead(&e.tagID); err != nil {
        return
    }
    if err = br.BRead(&e.typeID); err != nil {
        return
    }
    if err = br.BRead(&e.count); err != nil {
        return
    }
    if err = br.BRead(&e.valueOffset); err != nil {
        return
    }
    return e, nil
}
