// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

// A Field is primarily comprised of an Entry.  If the Entry's value is actually
// an offset to the data that the entry describes, then the Field will contain
// both the offset and the data that the offset points to in the file.

import (
    "encoding/binary"
    "encoding/json"
    "fmt"
    "reflect"
    "sync"
)

type FieldParser func(BReader, TagSpace, FieldTypeSpace) (Field, error)

var (
    tiffFieldPrintFullFieldValue bool
    printMu                      sync.RWMutex
)

func SetTiffFieldPrintFullFieldValue(b bool) {
    printMu.Lock()
    defer printMu.Unlock()
    tiffFieldPrintFullFieldValue = b
}

func GetTiffFieldPrintFullFieldValue() bool {
    printMu.RLock()
    defer printMu.RUnlock()
    return tiffFieldPrintFullFieldValue
}

type FieldValue interface {
    Order() binary.ByteOrder
    Bytes() []byte
}

type fieldValue struct {
    order binary.ByteOrder
    value []byte
}

func (fv *fieldValue) Order() binary.ByteOrder {
    return fv.order
}

func (fv *fieldValue) Bytes() []byte {
    return fv.value
}

func (fv *fieldValue) MarshalJSON() ([]byte, error) {
    tmp := struct {
        Bytes []byte
    }{
        Bytes: fv.value,
    }
    return json.Marshal(tmp)
}

// Field represents a field in an IFD in a TIFF file.
type Field interface {
    Tag() Tag
    Type() FieldType
    Count() uint64
    Offset() uint64
    Value() FieldValue // TODO: Change to BReader??
}

type field struct {
    entry Entry

    // If the offset from entry is actually a value, then the bytes will be
    // stored here and offset should be considered 0.  Otherwise, the offset
    // will indicate the location in the file where the values can be found.
    // Then value will be used to hold the bytes associated with those values.
    value FieldValue

    // ftsp is the FieldTypeSpace that can be used to look up the FieldType
    // that corresponds to the typeId of this entry.  If ftsp is nil, the
    // default set DefaultFieldTypeSpace is used instead.
    ftsp FieldTypeSpace

    // tsp is the TagSpace that can be used to look up the Tag that
    // corresponds to the result of entry.TagID().
    tsp TagSpace
}

func (f *field) Tag() Tag {
    if f.tsp == nil {
        return DefaultTagSpace.GetTag(f.entry.TagID())
    }
    return f.tsp.GetTag(f.entry.TagID())
}

func (f *field) Type() FieldType {
    if f.ftsp == nil {
        return DefaultFieldTypeSpace.GetFieldType(f.entry.TypeID())
    }
    return f.ftsp.GetFieldType(f.entry.TypeID())
}

func (f *field) Count() uint64 {
    return uint64(f.entry.Count())
}

func (f *field) Offset() uint64 {
    if f.Type().Size()*f.Count() <= 4 {
        return 0
    }
    offsetBytes := f.entry.ValueOffset()
    return uint64(f.value.Order().Uint32(offsetBytes[:]))
}

func (f *field) Value() FieldValue {
    return f.value
}

func (f *field) String() string {
    var (
        theTSP  = f.tsp
        theFTSP = f.ftsp
    )
    if theTSP == nil {
        theTSP = DefaultTagSpace
    }
    if theFTSP == nil {
        theFTSP = DefaultFieldTypeSpace
    }
    var valueRep string
    switch f.Type().ReflectType().Kind() {
    case reflect.String:
        if GetTiffFieldPrintFullFieldValue() {
            valueRep = fmt.Sprintf("%q", f.value.Bytes()[:f.Count()])
        } else {
            if f.Count() > 40 {
                valueRep = fmt.Sprintf("%q...", f.value.Bytes()[:41])
            } else {
                valueRep = fmt.Sprintf("%q", f.value.Bytes()[:f.Count()])
            }
        }
    default:
        // For general display purposes, don't show more than maxItems
        // amount of elements.  In this case, 10 is reasonable.  Beyond
        // 10 starts to line wrap in some cases like rationals.  If
        // we encounter that the value contains more than 10, we append
        // the ... to the end during string formatting to indicate that
        // there were more values, but they are not displayed here.
        const maxItems = 10
        buf := f.Value().Bytes()
        size := f.Type().Size()
        count := f.Count()
        if !GetTiffFieldPrintFullFieldValue() {
            if count > maxItems {
                count = maxItems
                buf = buf[:count*size]
            }
        }
        vals := make([]string, 0, count)
        for len(buf) > 0 {
            if f.Type().Repr() != nil {
                vals = append(vals, f.Type().Repr()(buf[:size], f.Value().Order()))
            } else {
                vals = append(vals, fmt.Sprintf("%v", buf[:size]))
            }
            buf = buf[size:]
        }
        if count == 1 {
            valueRep = vals[0]
        } else {
            if GetTiffFieldPrintFullFieldValue() {
                valueRep = fmt.Sprintf("%v", vals[:f.Count()])
            } else {
                // Keep a limit of 40 base characters when printing
                var totalLen, stop int
                for i := range vals {
                    totalLen += len(vals[i])
                    if totalLen > 40 {
                        stop = i
                        break
                    }
                    totalLen += 1 // account for space between values
                }
                if stop > 0 {
                    vals = vals[:stop]
                }
                if stop > 0 || f.Count() > maxItems {
                    valueRep = fmt.Sprintf("%v...", vals)
                } else {
                    valueRep = fmt.Sprintf("%v", vals[:f.Count()])
                }
            }
        }
    }
    tagID := f.Tag().ID()
    return fmt.Sprintf(`<Tag: (%#04x/%05[1]d) %v    Type: (%02d) %v    Count: %d    Offset: %d    Value: %s    FieldTypeSpace: %q    TagSpaceSet: "%s.%s">`,
        tagID, f.Tag().Name(), f.Type().ID(), f.Type().Name(), f.Count(), f.Offset(), valueRep,
        theFTSP.Name(), theTSP.Name(), theTSP.GetTagSetNameFromTag(tagID))
}

func (f *field) MarshalJSON() ([]byte, error) {
    tmp := struct {
        E    Entry          `json:"Entry"`
        V    FieldValue     `json:"FieldValue"`
        FTSP FieldTypeSpace `json:"FieldTypeSpace"`
        TSP  TagSpace       `json:"TagSpace"`
    }{
        E:    f.entry,
        V:    f.value,
        FTSP: f.ftsp,
        TSP:  f.tsp,
    }
    return json.Marshal(tmp)
}

func ParseField(br BReader, tsp TagSpace, ftsp FieldTypeSpace) (out Field, err error) {
    if ftsp == nil {
        ftsp = DefaultFieldTypeSpace
    }
    if tsp == nil {
        tsp = DefaultTagSpace
    }
    f := &field{ftsp: ftsp, tsp: tsp}
    if f.entry, err = ParseEntry(br); err != nil {
        return
    }
    fv := &fieldValue{order: br.ByteOrder()}
    valSize := int64(f.Count()) * int64(f.Type().Size())
    valOffBytes := f.entry.ValueOffset()
    if valSize > 4 {
        fv.value = make([]byte, valSize)
        offset := int64(br.ByteOrder().Uint32(valOffBytes[:]))
        if err = br.BReadSection(&fv.value, offset, valSize); err != nil {
            return
        }
    } else {
        fv.value = valOffBytes[:]
    }
    f.value = fv
    return f, nil
}
