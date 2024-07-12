// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bigtiff

import (
    "encoding/binary"
    "encoding/json"
    "fmt"
    "reflect"
    "sync"

    "github.com/google/tiff"
)

// A Field is primarily comprised of an Entry.  If the Entry's value is actually
// an offset to the data that the entry describes, then the Field will contain
// both the offset and the data that the offset points to in the file.

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

type field struct {
    entry Entry

    // If the offset entry is actually a value, then the bytes will be
    // stored here and offset should be set to 0.  Otherwise, the offset
    // will indicate the location in the file where the values can be found.
    // Then value will hold the bytes associated with those values.
    value tiff.FieldValue

    // ftsp is the FieldTypeSpace that can be used to look up the FieldType
    // that corresponds to the typeId of this entry.  If ftsp is nil, the
    // default set tiff.DefaultFieldTypeSpace is used instead.
    ftsp tiff.FieldTypeSpace

    // tsp is the TagSpace that can be used to look up the Tag that
    // corresponds to the result of entry.TagID().
    tsp tiff.TagSpace
}

func (f *field) Tag() tiff.Tag {
    if f.tsp == nil {
        return tiff.DefaultTagSpace.GetTag(f.entry.TagID())
    }
    return f.tsp.GetTag(f.entry.TagID())
}

func (f *field) Type() tiff.FieldType {
    if f.ftsp == nil {
        return tiff.DefaultFieldTypeSpace.GetFieldType(f.entry.TypeID())
    }
    return f.ftsp.GetFieldType(f.entry.TypeID())
}

func (f *field) Count() uint64 {
    return f.entry.Count()
}

func (f *field) Offset() uint64 {
    if uint64(f.Type().Size())*f.Count() <= 8 {
        return 0
    }
    offsetBytes := f.entry.ValueOffset()
    return f.value.Order().Uint64(offsetBytes[:])
}

func (f *field) Value() tiff.FieldValue {
    return f.value
}

func (f *field) String() string {
    var (
        theTSP  = f.tsp
        theFTSP = f.ftsp
    )
    if theTSP == nil {
        theTSP = tiff.DefaultTagSpace
    }
    if theFTSP == nil {
        theFTSP = tiff.DefaultFieldTypeSpace
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
    return nil, nil
}

func ParseField(br tiff.BReader, tsp tiff.TagSpace, ftsp tiff.FieldTypeSpace) (out tiff.Field, err error) {
    if ftsp == nil {
        ftsp = tiff.DefaultFieldTypeSpace
    }
    if tsp == nil {
        tsp = tiff.DefaultTagSpace
    }
    f := &field{ftsp: ftsp, tsp: tsp}
    if f.entry, err = ParseEntry(br); err != nil {
        return
    }
    fv := &fieldValue{order: br.ByteOrder()}
    valSize := int64(f.Count()) * int64(f.Type().Size())
    valOffBytes := f.entry.ValueOffset()
    if valSize > 8 {
        fv.value = make([]byte, valSize)
        offset := int64(br.ByteOrder().Uint64(valOffBytes[:])) // Hope this does not go negative
        if err = br.BReadSection(&fv.value, offset, valSize); err != nil {
            return
        }
    } else {
        fv.value = valOffBytes[:]
    }
    f.value = fv
    return f, nil
}
