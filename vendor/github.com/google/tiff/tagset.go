// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tiff

import (
    "fmt"
    "sort"
    "sync"
)

type TagSet interface {
    GetTag(id uint16) (Tag, bool)
    ListTags() []uint16
    ListTagNames() []string
    Name() string
    Register(t Tag) bool
    Lock()
}

func NewTagSet(name string, lower, upper uint16) TagSet {
    return &tagSet{
        name:  name,
        lower: lower,
        upper: upper,
        tags:  make(map[uint16]Tag, 1),
    }
}

type tagSet struct {
    mu     sync.RWMutex
    locked bool
    name   string
    lower  uint16
    upper  uint16
    tags   map[uint16]Tag
}

func (ts *tagSet) GetTag(id uint16) (Tag, bool) {
    ts.mu.RLock()
    t, ok := ts.tags[id]
    ts.mu.RUnlock()
    return t, ok
}

func (ts *tagSet) ListTags() []uint16 {
    ts.mu.RLock()
    ids := make([]uint16, len(ts.tags))
    i := 0
    for id := range ts.tags {
        ids[i] = id
        i++
    }
    sort.Sort(uint16Slice(ids))
    ts.mu.RUnlock()
    return ids
}

func (ts *tagSet) ListTagNames() []string {
    ts.mu.RLock()
    names := make([]string, len(ts.tags))
    i := 0
    for _, t := range ts.tags {
        names[i] = t.Name()
        i++
    }
    sort.Strings(names)
    ts.mu.RUnlock()
    return names
}

func (ts *tagSet) Name() string {
    return ts.name
}

func (ts *tagSet) Register(t Tag) bool {
    ts.mu.Lock()
    defer ts.mu.Unlock()
    // Disallow registration if the set is locked.
    if ts.locked {
        return false
    }
    id := t.ID()
    // Disallow registration if the id is not within the lower & upper bounds.
    if id < ts.lower || id > ts.upper {
        return false
    }
    // Just overwrite tags if they already exist.  Users doing this
    // should be generally aware of what they are doing.
    ts.tags[id] = t
    return true
}

func (ts *tagSet) Lock() {
    ts.mu.Lock()
    ts.locked = true
    ts.mu.Unlock()
}

func (ts *tagSet) String() string {
    return fmt.Sprintf("<TagSet: %q>", ts.name)
}
