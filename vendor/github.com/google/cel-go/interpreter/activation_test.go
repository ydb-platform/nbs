// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package interpreter

import (
    "testing"
    "time"

    "github.com/google/cel-go/common/types"
    "github.com/google/cel-go/common/types/ref"
)

func TestActivation(t *testing.T) {
    act, err := NewActivation(map[string]any{"a": types.True})
    if err != nil {
        t.Fatalf("Got err: %v, wanted activation", err)
    }
    _, err = NewActivation(act)
    if err != nil {
        t.Fatalf("Got err: %v, wanted activation", err)
    }
    act3, err := NewActivation("")
    if err == nil {
        t.Fatalf("Got %v, wanted err", act3)
    }
}

func TestActivation_Resolve(t *testing.T) {
    activation, _ := NewActivation(map[string]any{"a": types.True})
    if val, found := activation.ResolveName("a"); !found || val != types.True {
        t.Error("Activation failed to resolve 'a'")
    }
}

func TestActivation_ResolveLazy(t *testing.T) {
    var v ref.Val
    now := func() ref.Val {
        if v == nil {
            v = types.DefaultTypeAdapter.NativeToValue(time.Now().Unix())
        }
        return v
    }
    a, _ := NewActivation(map[string]any{
        "now": now,
    })
    first, _ := a.ResolveName("now")
    second, _ := a.ResolveName("now")
    if first != second {
        t.Errorf("Got different second, "+
            "expected same as first: 1:%v 2:%v", first, second)
    }
}

func TestHierarchicalActivation(t *testing.T) {
    // compose a parent with more properties than the child
    parent, _ := NewActivation(map[string]any{
        "a": types.String("world"),
        "b": types.Int(-42),
    })
    // compose the child such that it shadows the parent
    child, _ := NewActivation(map[string]any{
        "a": types.True,
        "c": types.String("universe"),
    })
    combined := NewHierarchicalActivation(parent, child)

    // Resolve the shadowed child value.
    if val, found := combined.ResolveName("a"); !found || val != types.True {
        t.Error("Activation failed to resolve shadow value of 'a'")
    }
    // Resolve the parent only value.
    if val, found := combined.ResolveName("b"); !found || val.(types.Int) != -42 {
        t.Error("Activation failed to resolve parent value of 'b'")
    }
    // Resolve the child only value.
    if val, found := combined.ResolveName("c"); !found || val.(types.String) != "universe" {
        t.Error("Activation failed to resolve child value of 'c'")
    }
}
