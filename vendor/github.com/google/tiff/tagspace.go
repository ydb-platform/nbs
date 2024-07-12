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

// A TagSpace represents a group of TagSet where each of the tags from one
// TagSet should not collide with any of the tags from another TagSet.
type TagSpace interface {
    Name() string
    GetTag(id uint16) Tag
    GetTagSet(name string) (TagSet, bool)
    GetTagSetNameFromTag(id uint16) string
    ListTagSets() []string
    RegisterTagSet(ts TagSet)
}

func NewTagSpace(name string) TagSpace {
    return &tagSpace{
        name: name,
        ts:   make(map[string]TagSet, 1),
    }
}

type nsTagPair struct {
    tagSetName string
    tag        Tag
}

type tagSpace struct {
    mu   sync.RWMutex
    name string
    ts   map[string]TagSet
    tags [65536]*nsTagPair // Cache for fast lookup
}

func (tsp *tagSpace) Name() string {
    return tsp.name
}

func (tsp *tagSpace) GetTag(id uint16) Tag {
    tsp.mu.RLock()
    defer tsp.mu.RUnlock()
    // Fast lookup from cache
    if nstp := tsp.tags[id]; nstp != nil {
        return nstp.tag
    }
    // Slower lookup from map
    for _, ts := range tsp.ts {
        if t, ok := ts.GetTag(id); ok {
            // Cache it for faster future lookups
            tsp.tags[id] = &nsTagPair{ts.Name(), t}
            return t
        }
    }
    return NewTag(id, fmt.Sprintf("UNKNOWN_TAG_%d", id), nil)
}

func (tsp *tagSpace) GetTagSet(name string) (TagSet, bool) {
    tsp.mu.RLock()
    ts, ok := tsp.ts[name]
    tsp.mu.RUnlock()
    return ts, ok
}

func (tsp *tagSpace) GetTagSetNameFromTag(id uint16) string {
    tsp.mu.RLock()
    defer tsp.mu.RUnlock()
    // Fast lookup from cache
    if nstp := tsp.tags[id]; nstp != nil {
        return nstp.tagSetName
    }
    // Slower lookup from map
    for _, ts := range tsp.ts {
        if t, ok := ts.GetTag(id); ok {
            // Cache it for faster future lookups
            tsp.tags[id] = &nsTagPair{ts.Name(), t}
            return ts.Name()
        }
    }
    return ""
}

func (tsp *tagSpace) ListTagSets() []string {
    tsp.mu.RLock()
    names := make([]string, len(tsp.ts))
    i := 0
    for name := range tsp.ts {
        names[i] = name
        i++
    }
    tsp.mu.RUnlock()
    sort.Strings(names)
    return names
}

func (tsp *tagSpace) RegisterTagSet(ts TagSet) {
    tsp.mu.Lock()
    tsp.ts[ts.Name()] = ts
    for _, tID := range ts.ListTags() {
        t, _ := ts.GetTag(tID)
        // See if a tag already exists
        if nstp := tsp.tags[tID]; nstp != nil {
            // If the name is not the same, log a warning.
            if nstp.tag.Name() != t.Name() {
                log.Printf("tiff: registration warning: space %q: tag %d: %q in set %q conflicts with existing %q in set %q\n",
                    tsp.name, tID, t.Name(), ts.Name(), nstp.tag.Name(), nstp.tagSetName)
            }
            // If the name is the same, we do not care.
        }
        // If the tag already exists, it will be overwritten in the
        // cache for fast lookup.  We leave it up to users of this
        // package to understand when they write over existing tags.  A
        // user can always get the TagSet and then access the
        // conflicting tag that way.
        tsp.tags[tID] = &nsTagPair{ts.Name(), t}
    }
    tsp.mu.Unlock()
}

func (tsp *tagSpace) MarshalJSON() ([]byte, error) {
    tmp := struct {
        Name string
    }{
        Name: tsp.name,
    }
    return json.Marshal(tmp)
}

func (tsp *tagSpace) String() string {
    return fmt.Sprintf("<TagSpace: %q>", tsp.name)
}

//
var DefaultTagSpace = NewTagSpace("Default")

// RegisterTagSet registers a TagSet in the DefaultTagSpace.  Only tags that
// would not cause collisions should be registered this way (i.e. GPS and
// MakerNote tags would cause collisions with the default tiff tag space.)
func RegisterTagSet(ts TagSet) {
    DefaultTagSpace.RegisterTagSet(ts)
}

var allTagSpaceMap = struct {
    mu   sync.RWMutex
    list map[string]TagSpace
}{
    list: make(map[string]TagSpace, 1),
}

func RegisterTagSpace(tsp TagSpace) {
    allTagSpaceMap.mu.Lock()
    allTagSpaceMap.list[tsp.Name()] = tsp
    allTagSpaceMap.mu.Unlock()
}

func GetTagSpace(name string) TagSpace {
    allTagSpaceMap.mu.RLock()
    defer allTagSpaceMap.mu.RUnlock()
    return allTagSpaceMap.list[name]
}

func ListTagSpaceNames() []string {
    allTagSpaceMap.mu.RLock()
    defer allTagSpaceMap.mu.RUnlock()
    names := make([]string, 0, len(allTagSpaceMap.list))
    for k := range allTagSpaceMap.list {
        names = append(names, k)
    }
    sort.Strings(names)
    return names
}
