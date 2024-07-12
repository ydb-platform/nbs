// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

import (
    "encoding/binary"
    "encoding/json"
    "fmt"
    "reflect"
)

// A FieldType represents all of the necessary pieces of information one needs to
// know about a field type including a function that knows how to represent that
// type of data in an often human readable string.  Other string representation
// formats could be implemented (json, xml, etc).  Field types themselves have no
// actual stored value inside a TIFF.  They are here to help an implementer or
// user understand their format.
type FieldType interface {
    ID() uint16
    Name() string
    Size() uint64
    Signed() bool
    ReflectType() reflect.Type
    Repr() FieldTypeRepr
    Valuer() FieldTypeValuer
}

func NewFieldType(id uint16, name string, size uint64, signed bool, repr FieldTypeRepr, rval FieldTypeValuer, typ reflect.Type) FieldType {
    return &fieldType{
        id:     id,
        name:   name,
        size:   size,
        signed: signed,
        repr:   repr,
        rval:   rval,
        typ:    typ,
    }
}

type FieldTypeRepr func([]byte, binary.ByteOrder) string

type FieldTypeValuer func([]byte, binary.ByteOrder) reflect.Value

type fieldType struct {
    id     uint16
    name   string
    size   uint64
    signed bool
    repr   FieldTypeRepr
    rval   FieldTypeValuer
    typ    reflect.Type
}

func (ft *fieldType) ID() uint16 {
    return ft.id
}

func (ft *fieldType) Name() string {
    if len(ft.name) > 0 {
        return ft.name
    }
    return fmt.Sprintf("UNNAMED_FIELD_TYPE_%d", ft.id)
}

func (ft *fieldType) Size() uint64 {
    return ft.size
}

func (ft *fieldType) Signed() bool {
    return ft.signed
}

func (ft *fieldType) ReflectType() reflect.Type {
    return ft.typ
}

func (ft *fieldType) Repr() FieldTypeRepr {
    return ft.repr
}

func (ft *fieldType) Valuer() FieldTypeValuer {
    return ft.rval
}

func (ft *fieldType) MarshalJSON() ([]byte, error) {
    tmp := struct {
        ID     uint16
        Name   string
        Size   uint64
        Signed bool
    }{
        ID:     ft.id,
        Name:   ft.name,
        Size:   ft.size,
        Signed: ft.signed,
    }
    return json.Marshal(tmp)
}
