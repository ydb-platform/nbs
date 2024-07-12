// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

import (
    "encoding/json"
    "fmt"
    "sort"
    "sync"
)

// FieldTypeSet represents a set of field types that may be in use within a file
// that uses a TIFF file structure.  This can be customized for custom file
// formats and private IFDs.
type FieldTypeSet interface {
    GetFieldType(id uint16) (FieldType, bool)
    ListFieldTypes() []uint16
    ListFieldTypeNames() []string
    Name() string
    Register(ft FieldType) bool
    Lock()
}

func NewFieldTypeSet(name string) FieldTypeSet {
    return &fieldTypeSet{
        name:  name,
        types: make(map[uint16]FieldType, 1),
    }
}

type fieldTypeSet struct {
    mu     sync.RWMutex
    locked bool
    name   string
    types  map[uint16]FieldType
}

func (fts *fieldTypeSet) GetFieldType(id uint16) (FieldType, bool) {
    fts.mu.RLock()
    ft, ok := fts.types[id]
    fts.mu.RUnlock()
    return ft, ok
}

func (fts *fieldTypeSet) ListFieldTypes() []uint16 {
    fts.mu.RLock()
    ids := make([]uint16, len(fts.types))
    i := 0
    for id := range fts.types {
        ids[i] = id
        i++
    }
    sort.Sort(uint16Slice(ids))
    fts.mu.RUnlock()
    return ids
}

func (fts *fieldTypeSet) ListFieldTypeNames() []string {
    fts.mu.RLock()
    names := make([]string, len(fts.types))
    i := 0
    for _, ft := range fts.types {
        names[i] = ft.Name()
        i++
    }
    sort.Strings(names)
    fts.mu.RUnlock()
    return names
}

func (fts *fieldTypeSet) Name() string {
    return fts.name
}

func (fts *fieldTypeSet) Register(ft FieldType) bool {
    fts.mu.Lock()
    defer fts.mu.Unlock()
    // Disallow registration if the set is locked.
    if fts.locked {
        return false
    }
    // Just overwrite field types if they already exist.  Users doing this
    // should be generally aware of what they are doing.
    fts.types[ft.ID()] = ft
    return true
}

func (fts *fieldTypeSet) Lock() {
    fts.mu.Lock()
    fts.locked = true
    fts.mu.Unlock()
}

func (fts *fieldTypeSet) MarshalJSON() ([]byte, error) {
    tmp := struct {
        Name string
    }{
        Name: fts.name,
    }
    return json.Marshal(tmp)
}

func (fts *fieldTypeSet) String() string {
    return fmt.Sprintf("<FieldTypeSet: %q>", fts.name)
}
