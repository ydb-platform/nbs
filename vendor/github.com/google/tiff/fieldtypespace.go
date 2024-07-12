// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

import (
    "encoding/json"
    "fmt"
    "log"
    "sort"
    "sync"
)

// A FieldTypeSpace represents a group of FieldTypeSets where each of the FieldTypes from one
// FieldTypeSet should not collide with any of the FieldTypes from another FieldTypeSet.
type FieldTypeSpace interface {
    Name() string
    GetFieldType(id uint16) FieldType
    GetFieldTypeSet(name string) (FieldTypeSet, bool)
    ListFieldTypeSets() []string
    RegisterFieldTypeSet(fts FieldTypeSet)
}

func NewFieldTypeSpace(name string) FieldTypeSpace {
    return &fieldTypeSpace{
        name: name,
        fts:  make(map[string]FieldTypeSet, 1),
    }
}

type nsFieldTypePair struct {
    ftsName string
    ft      FieldType
}

type fieldTypeSpace struct {
    mu         sync.RWMutex
    name       string
    fts        map[string]FieldTypeSet
    fieldTypes [65536]*nsFieldTypePair // Cache for fast lookup
}

func (ftsp *fieldTypeSpace) Name() string {
    return ftsp.name
}

func (ftsp *fieldTypeSpace) GetFieldType(id uint16) FieldType {
    ftsp.mu.RLock()
    defer ftsp.mu.RUnlock()
    // Fast lookup from cache
    if nsftp := ftsp.fieldTypes[id]; nsftp != nil {
        return nsftp.ft
    }
    // Slower lookup from map
    for _, fts := range ftsp.fts {
        if ft, ok := fts.GetFieldType(id); ok {
            // Cache it for faster future lookups
            ftsp.fieldTypes[id] = &nsFieldTypePair{fts.Name(), ft}
            return ft
        }
    }
    // For unknown field types, just represent them as bytes.
    return NewFieldType(id, fmt.Sprintf("UNKNOWN_FIELDTYPE_%d", id), 1, false, reprByte, rvalByte, typByte)
}

func (ftsp *fieldTypeSpace) GetFieldTypeSet(name string) (FieldTypeSet, bool) {
    ftsp.mu.RLock()
    fts, ok := ftsp.fts[name]
    ftsp.mu.RUnlock()
    return fts, ok
}

func (ftsp *fieldTypeSpace) ListFieldTypeSets() []string {
    ftsp.mu.RLock()
    names := make([]string, len(ftsp.fts))
    i := 0
    for name := range ftsp.fts {
        names[i] = name
        i++
    }
    ftsp.mu.RUnlock()
    sort.Strings(names)
    return names
}

func (ftsp *fieldTypeSpace) RegisterFieldTypeSet(fts FieldTypeSet) {
    ftsp.mu.Lock()
    ftsp.fts[fts.Name()] = fts
    for _, ftID := range fts.ListFieldTypes() {
        ft, _ := fts.GetFieldType(ftID)
        // See if a FieldType already exists
        if nsftp := ftsp.fieldTypes[ftID]; nsftp != nil {
            // If the name is not the same, log a warning.
            if nsftp.ft.Name() != ft.Name() {
                log.Printf("tiff: registration warning: space %q: fieldtype %d: %q in set %q conflicts with existing %q in set %q\n",
                    ftsp.name, ftID, ft.Name(), fts.Name(), nsftp.ft.Name(), nsftp.ftsName)
            }
            // If the name is the same, we do not care.
        }
        // If the FieldType already exists, it will be overwritten in the
        // cache for fast lookup.  We leave it up to users of this
        // package to understand when they write over existing FieldTypes.  A
        // user can always get the FieldTypeSet and then access the
        // conflicting FieldType that way.
        ftsp.fieldTypes[ftID] = &nsFieldTypePair{fts.Name(), ft}
    }
    ftsp.mu.Unlock()
}

func (ftsp *fieldTypeSpace) MarshalJSON() ([]byte, error) {
    tmp := struct {
        Name string
    }{
        Name: ftsp.name,
    }
    return json.Marshal(tmp)
}

func (ftsp *fieldTypeSpace) String() string {
    return fmt.Sprintf("<FieldTypeSpace: %q>", ftsp.name)
}

//
var DefaultFieldTypeSpace = NewFieldTypeSpace("Default")

// RegisterFieldTypeSet registers a FieldTypeSet in the DefaultFieldTypeSpace.
// Only FieldTypes that would not cause collisions should be registered this way.
func RegisterFieldTypeSet(fts FieldTypeSet) {
    DefaultFieldTypeSpace.RegisterFieldTypeSet(fts)
}

var allFieldTypeSpaceMap = struct {
    mu   sync.RWMutex
    list map[string]FieldTypeSpace
}{
    list: make(map[string]FieldTypeSpace, 1),
}

func RegisterFieldTypeSpace(ftsp FieldTypeSpace) {
    allFieldTypeSpaceMap.mu.Lock()
    allFieldTypeSpaceMap.list[ftsp.Name()] = ftsp
    allFieldTypeSpaceMap.mu.Unlock()
}

func GetFieldTypeSpace(name string) FieldTypeSpace {
    allFieldTypeSpaceMap.mu.RLock()
    defer allFieldTypeSpaceMap.mu.RUnlock()
    return allFieldTypeSpaceMap.list[name]
}
