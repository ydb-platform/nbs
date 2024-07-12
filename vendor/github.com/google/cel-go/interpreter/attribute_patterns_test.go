// Copyright 2020 Google LLC
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
    "fmt"
    "testing"

    "github.com/google/cel-go/common/containers"
    "github.com/google/cel-go/common/types"
)

// attr describes a simplified format for specifying common Attribute and Qualifier values for
// use in pattern matching tests.
type attr struct {
    // unchecked indicates whether the attribute has not been type-checked and thus not gone
    // the variable and function resolution step.
    unchecked bool
    // container simulates the expression container and is only relevant on 'unchecked' test inputs
    // as the container is used to resolve the potential fully qualified variable names represented
    // by an identifier or select expression.
    container string
    // variable name, fully qualified unless the attr is marked as unchecked=true
    name string
    // quals contains a list of static qualifiers.
    quals []any
}

// patternTest describes a pattern, and a set of matches and misses for the pattern to highlight
// what the pattern will and will not match.
type patternTest struct {
    pattern *AttributePattern
    matches []attr
    misses  []attr
}

var patternTests = map[string]patternTest{
    "var": {
        pattern: NewAttributePattern("var"),
        matches: []attr{
            {name: "var"},
            {name: "var", quals: []any{"field"}},
        },
        misses: []attr{
            {name: "ns.var"},
        },
    },
    "var_namespace": {
        pattern: NewAttributePattern("ns.app.var"),
        matches: []attr{
            {name: "ns.app.var"},
            {name: "ns.app.var", quals: []any{int64(0)}},
            {
                name:      "ns",
                quals:     []any{"app", "var", "foo"},
                container: "ns.app",
                unchecked: true,
            },
        },
        misses: []attr{
            {name: "ns.var"},
            {
                name:      "ns",
                quals:     []any{"var"},
                container: "ns.app",
                unchecked: true,
            },
        },
    },
    "var_field": {
        pattern: NewAttributePattern("var").QualString("field"),
        matches: []attr{
            {name: "var"},
            {name: "var", quals: []any{"field"}},
            {name: "var", quals: []any{"field"}, unchecked: true},
            {name: "var", quals: []any{"field", uint64(1)}},
        },
        misses: []attr{
            {name: "var", quals: []any{"other"}},
        },
    },
    "var_index": {
        pattern: NewAttributePattern("var").QualInt(0),
        matches: []attr{
            {name: "var"},
            {name: "var", quals: []any{int64(0)}},
            {name: "var", quals: []any{float64(0)}},
            {name: "var", quals: []any{int64(0), false}},
            {name: "var", quals: []any{uint64(0)}},
        },
        misses: []attr{
            {name: "var", quals: []any{int64(1), false}},
        },
    },
    "var_index_uint": {
        pattern: NewAttributePattern("var").QualUint(1),
        matches: []attr{
            {name: "var"},
            {name: "var", quals: []any{uint64(1)}},
            {name: "var", quals: []any{uint64(1), true}},
            {name: "var", quals: []any{int64(1), false}},
        },
        misses: []attr{
            {name: "var", quals: []any{uint64(0)}},
        },
    },
    "var_index_bool": {
        pattern: NewAttributePattern("var").QualBool(true),
        matches: []attr{
            {name: "var"},
            {name: "var", quals: []any{true}},
            {name: "var", quals: []any{true, "name"}},
        },
        misses: []attr{
            {name: "var", quals: []any{false}},
            {name: "none"},
        },
    },
    "var_wildcard": {
        pattern: NewAttributePattern("ns.var").Wildcard(),
        matches: []attr{
            {name: "ns.var"},
            // The unchecked attributes consider potential namespacing and field selection
            // when testing variable names.
            {
                name:      "var",
                quals:     []any{true},
                container: "ns",
                unchecked: true,
            },
            {
                name:      "var",
                quals:     []any{"name"},
                container: "ns",
                unchecked: true,
            },
            {
                name:      "var",
                quals:     []any{"name"},
                container: "ns",
                unchecked: true,
            },
        },
        misses: []attr{
            {name: "var", quals: []any{false}},
            {name: "none"},
        },
    },
    "var_wildcard_field": {
        pattern: NewAttributePattern("var").Wildcard().QualString("field"),
        matches: []attr{
            {name: "var"},
            {name: "var", quals: []any{true}},
            {name: "var", quals: []any{int64(10), "field"}},
        },
        misses: []attr{
            {name: "var", quals: []any{int64(10), "other"}},
        },
    },
    "var_wildcard_wildcard": {
        pattern: NewAttributePattern("var").Wildcard().Wildcard(),
        matches: []attr{
            {name: "var"},
            {name: "var", quals: []any{true}},
            {name: "var", quals: []any{int64(10), "field"}},
        },
        misses: []attr{
            {name: "none"},
        },
    },
}

func TestAttributePattern_UnknownResolution(t *testing.T) {
    reg := newTestRegistry(t)
    for nm, tc := range patternTests {
        tst := tc
        t.Run(nm, func(t *testing.T) {
            for i, match := range tst.matches {
                m := match
                t.Run(fmt.Sprintf("match[%d]", i), func(t *testing.T) {
                    var err error
                    cont := containers.DefaultContainer
                    if m.unchecked {
                        cont, err = containers.NewContainer(containers.Name(m.container))
                        if err != nil {
                            t.Fatal(err)
                        }
                    }
                    fac := NewPartialAttributeFactory(cont, reg, reg)
                    attr := genAttr(fac, m)
                    partVars, _ := NewPartialActivation(EmptyActivation(), tst.pattern)
                    val, err := attr.Resolve(partVars)
                    if err != nil {
                        t.Fatalf("Got error: %s, wanted unknown", err)
                    }
                    _, isUnk := val.(*types.Unknown)
                    if !isUnk {
                        t.Fatalf("Got value %v, wanted unknown", val)
                    }
                })
            }
            for i, miss := range tst.misses {
                m := miss
                t.Run(fmt.Sprintf("miss[%d]", i), func(t *testing.T) {
                    cont := containers.DefaultContainer
                    if m.unchecked {
                        var err error
                        cont, err = containers.NewContainer(containers.Name(m.container))
                        if err != nil {
                            t.Fatal(err)
                        }
                    }
                    fac := NewPartialAttributeFactory(cont, reg, reg)
                    attr := genAttr(fac, m)
                    partVars, _ := NewPartialActivation(EmptyActivation(), tst.pattern)
                    val, err := attr.Resolve(partVars)
                    if err == nil {
                        t.Fatalf("Got value: %s, wanted error", val)
                    }
                })
            }
        })
    }
}

func TestAttributePattern_CrossReference(t *testing.T) {
    reg := newTestRegistry(t)
    fac := NewPartialAttributeFactory(containers.DefaultContainer, reg, reg)
    a := fac.AbsoluteAttribute(1, "a")
    b := fac.AbsoluteAttribute(2, "b")
    a.AddQualifier(b)

    // Ensure that var a[b], the dynamic index into var 'a' is the unknown value
    // returned from attribute resolution.
    partVars, _ := NewPartialActivation(
        map[string]any{"a": []int64{1, 2}},
        NewAttributePattern("b"))
    val, err := a.Resolve(partVars)
    if err != nil {
        t.Fatal(err)
    }
    if !types.NewUnknown(2, types.NewAttributeTrail("b")).Contains(val.(*types.Unknown)) {
        t.Errorf("Got %v, wanted unknown attribute id for 'b' (2)", val)
    }

    // Ensure that a[b], the dynamic index into var 'a' is the unknown value
    // returned from attribute resolution. Note, both 'a' and 'b' have unknown attribute
    // patterns specified. This changes the evaluation behavior slightly, but the end
    // result is the same.
    partVars, _ = NewPartialActivation(
        map[string]any{"a": []int64{1, 2}},
        NewAttributePattern("a").QualInt(0),
        NewAttributePattern("b"))
    val, err = a.Resolve(partVars)
    if err != nil {
        t.Fatal(err)
    }
    if !types.NewUnknown(2, types.NewAttributeTrail("b")).Contains(val.(*types.Unknown)) {
        t.Errorf("Got %v, wanted unknown attribute id for 'b' (2)", val)
    }

    // Note, that only 'a[0].c' will result in an unknown result since both 'a' and 'b'
    // have values. However, since the attribute being pattern matched is just 'a.b',
    // the outcome will indicate that 'a[b]' is unknown.
    partVars, _ = NewPartialActivation(
        map[string]any{"a": []int64{1, 2}, "b": 0},
        NewAttributePattern("a").QualInt(0).QualString("c"))
    val, err = a.Resolve(partVars)
    if err != nil {
        t.Fatal(err)
    }
    unkAttr := types.NewAttributeTrail("a")
    types.QualifyAttribute[int64](unkAttr, 0)
    wantUnk := types.NewUnknown(2, unkAttr)
    if !wantUnk.Contains(val.(*types.Unknown)) {
        t.Errorf("Got %v, wanted unknown attribute id for %v", val, wantUnk)
    }

    // Test a positive case that returns a valid value even though the attribugte factory
    // is the partial attribute factory.
    partVars, _ = NewPartialActivation(
        map[string]any{"a": []int64{1, 2}, "b": 0})
    val, err = a.Resolve(partVars)
    if err != nil {
        t.Fatal(err)
    }
    if val != int64(1) {
        t.Errorf("Got %v, wanted 1 for a[b]", val)
    }

    // Ensure the unknown attribute id moves when the attribute becomes more specific.
    partVars, _ = NewPartialActivation(
        map[string]any{"a": []int64{1, 2}, "b": 0},
        NewAttributePattern("a").QualInt(0).QualString("c"))
    // Qualify a[b] with 'c', a[b].c
    c, _ := fac.NewQualifier(nil, 3, "c", false)
    a.AddQualifier(c)
    // The resolve step should return unknown
    val, err = a.Resolve(partVars)
    if err != nil {
        t.Fatal(err)
    }
    unkAttr = types.NewAttributeTrail("a")
    types.QualifyAttribute[int64](unkAttr, 0)
    types.QualifyAttribute[string](unkAttr, "c")
    wantUnk = types.NewUnknown(3, unkAttr)
    if !wantUnk.Contains(val.(*types.Unknown)) {
        t.Errorf("Got %v, wanted unknown attribute id for %v", val, wantUnk)
    }
}

func genAttr(fac AttributeFactory, a attr) Attribute {
    id := int64(1)
    var attr Attribute
    if a.unchecked {
        attr = fac.MaybeAttribute(1, a.name)
    } else {
        attr = fac.AbsoluteAttribute(1, a.name)
    }
    for _, q := range a.quals {
        qual, _ := fac.NewQualifier(nil, id, q, false)
        attr.AddQualifier(qual)
        id++
    }
    return attr
}
