// Copyright 2023 Google LLC
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

package cel

import (
    "reflect"
    "sort"
    "testing"

    "google.golang.org/protobuf/encoding/prototext"
    "google.golang.org/protobuf/proto"

    "github.com/google/cel-go/common/ast"

    proto3pb "github.com/google/cel-go/test/proto3pb"
    exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func TestConstantFoldingOptimizer(t *testing.T) {
    tests := []struct {
        expr   string
        folded string
    }{
        {
            expr:   `[1, 1 + 2, 1 + (2 + 3)]`,
            folded: `[1, 3, 6]`,
        },
        {
            expr:   `6 in [1, 1 + 2, 1 + (2 + 3)]`,
            folded: `true`,
        },
        {
            expr:   `5 in [1, 1 + 2, 1 + (2 + 3)]`,
            folded: `false`,
        },
        {
            expr:   `x in [1, 1 + 2, 1 + (2 + 3)]`,
            folded: `x in [1, 3, 6]`,
        },
        {
            expr:   `1 in [1, x + 2, 1 + (2 + 3)]`,
            folded: `true`,
        },
        {
            expr:   `1 in [x, x + 2, 1 + (2 + 3)]`,
            folded: `1 in [x, x + 2, 6]`,
        },
        {
            expr:   `x in []`,
            folded: `false`,
        },
        {
            expr:   `{'hello': 'world'}.hello == x`,
            folded: `"world" == x`,
        },
        {
            expr:   `{'hello': 'world'}.?hello.orValue('default') == x`,
            folded: `"world" == x`,
        },
        {
            expr:   `{'hello': 'world'}['hello'] == x`,
            folded: `"world" == x`,
        },
        {
            expr:   `optional.of("hello")`,
            folded: `optional.of("hello")`,
        },
        {
            expr:   `optional.ofNonZeroValue("")`,
            folded: `optional.none()`,
        },
        {
            expr:   `{?'hello': optional.of('world')}['hello'] == x`,
            folded: `"world" == x`,
        },
        {
            expr:   `duration(string(7 * 24) + 'h')`,
            folded: `duration("604800s")`,
        },
        {
            expr:   `timestamp("1970-01-01T00:00:00Z")`,
            folded: `timestamp("1970-01-01T00:00:00Z")`,
        },
        {
            expr:   `[1, 1 + 1, 1 + 2, 2 + 3].exists(i, i < 10)`,
            folded: `true`,
        },
        {
            expr:   `[1, 1 + 1, 1 + 2, 2 + 3].exists(i, i < 1 % 2)`,
            folded: `false`,
        },
        {
            expr:   `[1, 2, 3].map(i, [1, 2, 3].map(j, i * j))`,
            folded: `[[1, 2, 3], [2, 4, 6], [3, 6, 9]]`,
        },
        {
            expr:   `[1, 2, 3].map(i, [1, 2, 3].map(j, i * j).filter(k, k % 2 == 0))`,
            folded: `[[2], [2, 4, 6], [6]]`,
        },
        {
            expr:   `[1, 2, 3].map(i, [1, 2, 3].map(j, i * j).filter(k, k % 2 == x))`,
            folded: `[1, 2, 3].map(i, [1, 2, 3].map(j, i * j).filter(k, k % 2 == x))`,
        },
        {
            expr:   `[{}, {"a": 1}, {"b": 2}].filter(m, has(m.a))`,
            folded: `[{"a": 1}]`,
        },
        {
            expr:   `[{}, {"a": 1}, {"b": 2}].filter(m, has({'a': true}.a))`,
            folded: `[{}, {"a": 1}, {"b": 2}]`,
        },
        {
            expr:   `type(1)`,
            folded: `int`,
        },
        {
            expr:   `[google.expr.proto3.test.TestAllTypes{single_int32: 2 + 3}].map(i, i)[0]`,
            folded: `google.expr.proto3.test.TestAllTypes{single_int32: 5}`,
        },
        {
            expr:   `[?optional.ofNonZeroValue(0)]`,
            folded: `[]`,
        },
        {
            expr:   `[1, ?optional.ofNonZeroValue(0)]`,
            folded: `[1]`,
        },
        {
            expr:   `[optional.none(), ?x]`,
            folded: `[optional.none(), ?x]`,
        },
        {
            expr:   `[?optional.none(), ?x]`,
            folded: `[?x]`,
        },
        {
            expr:   `[1, x, ?optional.ofNonZeroValue(0), ?x.?y]`,
            folded: `[1, x, ?x.?y]`,
        },
        {
            expr:   `[1, x, ?optional.ofNonZeroValue(3), ?x.?y]`,
            folded: `[1, x, 3, ?x.?y]`,
        },
        {
            expr:   `[1, x, ?optional.ofNonZeroValue(3), ?x.?y].size() > 3`,
            folded: `[1, x, 3, ?x.?y].size() > 3`,
        },
        {
            expr:   `{?'a': optional.of('hello'), ?x : optional.of(1), ?'b': optional.none()}`,
            folded: `{"a": "hello", ?x: optional.of(1)}`,
        },
        {
            expr:   `true ? x + 1 : x + 2`,
            folded: `x + 1`,
        },
        {
            expr:   `false ? x + 1 : x + 2`,
            folded: `x + 2`,
        },
        {
            expr:   `false ? x + 'world' : 'hello' + 'world'`,
            folded: `"helloworld"`,
        },
        {
            expr:   `true && x`,
            folded: `x`,
        },
        {
            expr:   `x && true`,
            folded: `x`,
        },
        {
            expr:   `false && x`,
            folded: `false`,
        },
        {
            expr:   `x && false`,
            folded: `false`,
        },
        {
            expr:   `true || x`,
            folded: `true`,
        },
        {
            expr:   `x || true`,
            folded: `true`,
        },
        {
            expr:   `false || x`,
            folded: `x`,
        },
        {
            expr:   `x || false`,
            folded: `x`,
        },
        {
            expr:   `true && x && true && x`,
            folded: `x && x`,
        },
        {
            expr:   `false || x || false || x`,
            folded: `x || x`,
        },
        {
            expr:   `true && true`,
            folded: `true`,
        },
        {
            expr:   `true && false`,
            folded: `false`,
        },
        {
            expr:   `true || false`,
            folded: `true`,
        },
        {
            expr:   `false || false`,
            folded: `false`,
        },
        {
            expr:   `true && false || true`,
            folded: `true`,
        },
        {
            expr:   `false && true || false`,
            folded: `false`,
        },
        {
            expr:   `null`,
            folded: `null`,
        },
        {
            expr:   `google.expr.proto3.test.TestAllTypes{?single_int32: optional.ofNonZeroValue(1)}`,
            folded: `google.expr.proto3.test.TestAllTypes{single_int32: 1}`,
        },
        {
            expr:   `google.expr.proto3.test.TestAllTypes{?single_int32: optional.ofNonZeroValue(0)}`,
            folded: `google.expr.proto3.test.TestAllTypes{}`,
        },
        {
            expr:   `google.expr.proto3.test.TestAllTypes{single_int32: x, repeated_int32: [1, 2, 3]}`,
            folded: `google.expr.proto3.test.TestAllTypes{single_int32: x, repeated_int32: [1, 2, 3]}`,
        },
        {
            expr:   `x + dyn([1, 2] + [3, 4])`,
            folded: `x + [1, 2, 3, 4]`,
        },
        {
            expr:   `dyn([1, 2]) + [3.0, 4.0]`,
            folded: `[1, 2, 3.0, 4.0]`,
        },
        {
            expr:   `{'a': dyn([1, 2]), 'b': x}`,
            folded: `{"a": [1, 2], "b": x}`,
        },
        {
            expr:   `1 + x + 2 == 2 + x + 1`,
            folded: `1 + x + 2 == 2 + x + 1`,
        },
        {
            // The order of operations makes it such that the appearance of x in the first means that
            // none of the values provided into the addition call will be folded with the current
            // implementation. Ideally, the result would be 3 + x == x + 3 (which could be trivially true
            // and more easily observed as a result of common subexpression eliminiation)
            expr:   `1 + 2 + x ==  x + 2 + 1`,
            folded: `3 + x == x + 2 + 1`,
        },
    }
    e, err := NewEnv(
        OptionalTypes(),
        EnableMacroCallTracking(),
        Types(&proto3pb.TestAllTypes{}),
        Variable("x", DynType))
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            checked, iss := e.Compile(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("Compile() failed: %v", iss.Err())
            }
            folder, err := NewConstantFoldingOptimizer()
            if err != nil {
                t.Fatalf("NewConstantFoldingOptimizer() failed: %v", err)
            }
            opt := NewStaticOptimizer(folder)
            optimized, iss := opt.Optimize(e, checked)
            if iss.Err() != nil {
                t.Fatalf("Optimize() generated an invalid AST: %v", iss.Err())
            }
            folded, err := AstToString(optimized)
            if err != nil {
                t.Fatalf("AstToString() failed: %v", err)
            }
            if folded != tc.folded {
                t.Errorf("got %q, wanted %q", folded, tc.folded)
            }
        })
    }
}

func TestConstantFoldingOptimizerMacroElimination(t *testing.T) {
    tests := []struct {
        expr       string
        folded     string
        macroCount int
    }{
        {
            expr:   `has({}.key)`,
            folded: `false`,
        },
        {
            expr:   `[1, 2, 3].filter(i, i < 1)`,
            folded: `[]`,
        },
        {
            expr:   `[{}, {"a": 1}, {"b": 2}].exists(i, has(i.b))`,
            folded: `true`,
        },
        {
            expr:       `has(x.b) && [{}, {"a": 1}, {"b": 2}].exists(i, has(i.b))`,
            folded:     `has(x.b)`,
            macroCount: 1,
        },
    }
    e, err := NewEnv(
        OptionalTypes(),
        EnableMacroCallTracking(),
        Types(&proto3pb.TestAllTypes{}),
        Variable("x", DynType))
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            checked, iss := e.Compile(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("Compile() failed: %v", iss.Err())
            }
            folder, err := NewConstantFoldingOptimizer()
            if err != nil {
                t.Fatalf("NewConstantFoldingOptimizer() failed: %v", err)
            }
            opt := NewStaticOptimizer(folder)
            optimized, iss := opt.Optimize(e, checked)
            if iss.Err() != nil {
                t.Fatalf("Optimize() generated an invalid AST: %v", iss.Err())
            }
            folded, err := AstToString(optimized)
            if err != nil {
                t.Fatalf("AstToString() failed: %v", err)
            }
            if folded != tc.folded {
                t.Errorf("folding got %q, wanted %q", folded, tc.folded)
            }
            if len(optimized.SourceInfo().GetMacroCalls()) != tc.macroCount {
                t.Errorf("folding got %d macros, wanted %d macros", len(optimized.SourceInfo().GetMacroCalls()), tc.macroCount)
            }
        })
    }
}

func TestConstantFoldingOptimizerWithLimit(t *testing.T) {
    tests := []struct {
        expr   string
        limit  int
        folded string
    }{
        {
            expr:   `[1, 1 + 2, 1 + (2 + 3)]`,
            limit:  1,
            folded: `[1, 3, 1 + 5]`,
        },
        {
            expr:   `5 in [1, 1 + 2, 1 + (2 + 3)]`,
            limit:  2,
            folded: `5 in [1, 3, 6]`,
        },
        {
            // though more complex, the final tryFold() at the end of the optimization pass
            // results in this computed output.
            expr:   `[1, 2, 3].map(i, [1, 2, 3].map(j, i * j))`,
            limit:  1,
            folded: `[[1, 2, 3], [2, 4, 6], [3, 6, 9]]`,
        },
    }
    e, err := NewEnv(
        OptionalTypes(),
        EnableMacroCallTracking(),
        Types(&proto3pb.TestAllTypes{}),
        Variable("x", DynType))
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            checked, iss := e.Compile(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("Compile() failed: %v", iss.Err())
            }
            folder, err := NewConstantFoldingOptimizer(MaxConstantFoldIterations(tc.limit))
            if err != nil {
                t.Fatalf("NewConstantFoldingOptimizer() failed: %v", err)
            }
            opt := NewStaticOptimizer(folder)
            optimized, iss := opt.Optimize(e, checked)
            if iss.Err() != nil {
                t.Fatalf("Optimize() generated an invalid AST: %v", iss.Err())
            }
            folded, err := AstToString(optimized)
            if err != nil {
                t.Fatalf("AstToString() failed: %v", err)
            }
            if folded != tc.folded {
                t.Errorf("got %q, wanted %q", folded, tc.folded)
            }
        })
    }
}

func TestConstantFoldingNormalizeIDs(t *testing.T) {
    tests := []struct {
        expr             string
        ids              []int64
        macros           map[int64]string
        normalizedIDs    []int64
        normalizedMacros map[int64]string
    }{
        {
            expr:          `[1, 2, 3]`,
            ids:           []int64{1, 2, 3, 4},
            normalizedIDs: []int64{1, 2, 3, 4},
        },
        {
            expr:          `google.expr.proto3.test.TestAllTypes{single_int32: 0}`,
            ids:           []int64{1, 2, 3},
            normalizedIDs: []int64{1, 2, 3},
        },
        {
            expr: `has({x: 'value'}.single_int32)`,
            ids:  []int64{2, 3, 4, 5, 7},
            macros: map[int64]string{7: `
            call_expr: {
                function: "has"
                args: {
                  id: 6
                  select_expr: {
                    operand: {
                      id: 2
                      struct_expr: {
                        entries: {
                          id: 3
                          map_key: {
                            id: 4
                            ident_expr: {
                              name: "x"
                            }
                          }
                          value: {
                            id: 5
                            const_expr: {
                              string_value: "value"
                            }
                          }
                        }
                      }
                    }
                    field: "single_int32"
                  }
                }
              }`},
            normalizedIDs: []int64{1, 2, 3, 4, 5},
            normalizedMacros: map[int64]string{1: `
            call_expr:  {
                function:  "has"
                args:  {
                  id:  6
                  select_expr:  {
                    operand:  {
                      id:  2
                      struct_expr:  {
                        entries:  {
                          id:  3
                          map_key:  {
                            id:  4
                            ident_expr:  {
                              name:  "x"
                            }
                          }
                          value:  {
                            id:  5
                            const_expr:  {
                              string_value:  "value"
                            }
                          }
                        }
                      }
                    }
                    field:  "single_int32"
                  }
                }
              }`,
            },
        },
        {
            expr: `has(google.expr.proto3.test.TestAllTypes{}.single_int32)`,
            ids:  []int64{2, 4},
            macros: map[int64]string{
                4: `call_expr:  {
                    function:  "has"
                    args:  {
                      id:  3
                      select_expr:  {
                        operand:  {
                          id:  2
                          struct_expr:  {
                            message_name:  "google.expr.proto3.test.TestAllTypes"
                          }
                        }
                        field:  "single_int32"
                      }
                    }
                  }`,
            },
            normalizedIDs: []int64{1},
        },
        {
            expr: `[true].exists(i, i)`,
            ids:  []int64{1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13},
            macros: map[int64]string{
                13: `call_expr:  {
                    target:  {
                      id:  1
                      list_expr:  {
                        elements:  {
                          id:  2
                          const_expr:  {
                            bool_value:  true
                          }
                        }
                      }
                    }
                    function:  "exists"
                    args:  {
                      id:  4
                      ident_expr:  {
                        name:  "i"
                      }
                    }
                    args:  {
                      id:  5
                      ident_expr:  {
                        name:  "i"
                      }
                    }
                  }`,
            },
            normalizedIDs: []int64{1},
        },
        {
            expr: `[x].exists(i, i)`,
            ids:  []int64{1, 2, 5, 6, 7, 8, 9, 10, 11, 12, 13},
            macros: map[int64]string{
                13: `call_expr:  {
                    target:  {
                      id:  1
                      list_expr:  {
                        elements:  {
                          id:  2
                          ident_expr:  {
                            name:  "x"
                          }
                        }
                      }
                    }
                    function:  "exists"
                    args:  {
                      id:  4
                      ident_expr:  {
                        name:  "i"
                      }
                    }
                    args:  {
                      id:  5
                      ident_expr:  {
                        name:  "i"
                      }
                    }
                  }`,
            },
            normalizedIDs: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
            normalizedMacros: map[int64]string{
                1: `call_expr: {
                    target: {
                      id: 2
                      list_expr: {
                        elements: {
                          id: 3
                          ident_expr: {
                            name: "x"
                          }
                        }
                      }
                    }
                    function: "exists"
                    args: {
                      id: 12
                      ident_expr: {
                        name: "i"
                      }
                    }
                    args: {
                      id: 10
                      ident_expr: {
                        name: "i"
                      }
                    }
                  }`,
            },
        },
    }
    e, err := NewEnv(
        EnableMacroCallTracking(),
        Types(&proto3pb.TestAllTypes{}),
        Variable("x", DynType))
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            checked, iss := e.Compile(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("Compile() failed: %v", iss.Err())
            }
            preOpt := newIDCollector()
            ast.PostOrderVisit(checked.impl.Expr(), preOpt)
            if !reflect.DeepEqual(preOpt.IDs(), tc.ids) {
                t.Errorf("Compile() got ids %v, expected %v", preOpt.IDs(), tc.ids)
            }
            for id, call := range checked.impl.SourceInfo().MacroCalls() {
                macroText, found := tc.macros[id]
                if !found {
                    t.Fatalf("Compile() did not find macro %d", id)
                }
                pbCall, err := ast.ExprToProto(call)
                if err != nil {
                    t.Fatalf("ast.ExprToProto() failed: %v", err)
                }
                pbMacro := &exprpb.Expr{}
                err = prototext.Unmarshal([]byte(macroText), pbMacro)
                if err != nil {
                    t.Fatalf("prototext.Unmarshal() failed: %v", err)
                }
                if !proto.Equal(pbCall, pbMacro) {
                    t.Errorf("Compile() for macro %d got %s, expected %s", id, prototext.Format(pbCall), macroText)
                }
            }
            folder, err := NewConstantFoldingOptimizer()
            if err != nil {
                t.Fatalf("NewConstantFoldingOptimizer() failed: %v", err)
            }
            opt := NewStaticOptimizer(folder)
            optimized, iss := opt.Optimize(e, checked)
            if iss.Err() != nil {
                t.Fatalf("Optimize() generated an invalid AST: %v", iss.Err())
            }
            postOpt := newIDCollector()
            ast.PostOrderVisit(optimized.impl.Expr(), postOpt)
            if !reflect.DeepEqual(postOpt.IDs(), tc.normalizedIDs) {
                t.Errorf("Optimize() got ids %v, expected %v", postOpt.IDs(), tc.normalizedIDs)
            }
            for id, call := range optimized.impl.SourceInfo().MacroCalls() {
                macroText, found := tc.normalizedMacros[id]
                if !found {
                    t.Fatalf("Optimize() did not find macro %d", id)
                }
                pbCall, err := ast.ExprToProto(call)
                if err != nil {
                    t.Fatalf("ast.ExprToProto() failed: %v", err)
                }
                pbMacro := &exprpb.Expr{}
                err = prototext.Unmarshal([]byte(macroText), pbMacro)
                if err != nil {
                    t.Fatalf("prototext.Unmarshal() failed: %v", err)
                }
                if !proto.Equal(pbCall, pbMacro) {
                    t.Errorf("Optimize() for macro %d got %s, expected %s", id, prototext.Format(pbCall), macroText)
                }
            }
        })
    }
}

func newIDCollector() *idCollector {
    return &idCollector{
        ids: int64Slice{},
    }
}

type idCollector struct {
    ids int64Slice
}

func (c *idCollector) VisitExpr(e ast.Expr) {
    if e.ID() == 0 {
        return
    }
    c.ids = append(c.ids, e.ID())
}

// VisitEntryExpr updates the max identifier if the incoming entry id is greater than previously observed.
func (c *idCollector) VisitEntryExpr(e ast.EntryExpr) {
    if e.ID() == 0 {
        return
    }
    c.ids = append(c.ids, e.ID())
}

func (c *idCollector) IDs() []int64 {
    sort.Sort(c.ids)
    return c.ids
}

// int64Slice is an implementation of the sort.Interface
type int64Slice []int64

// Len returns the number of elements in the slice.
func (x int64Slice) Len() int { return len(x) }

// Less indicates whether the value at index i is less than the value at index j.
func (x int64Slice) Less(i, j int) bool { return x[i] < x[j] }

// Swap swaps the values at indices i and j in place.
func (x int64Slice) Swap(i, j int) { x[i], x[j] = x[j], x[i] }

// Sort is a convenience method: x.Sort() calls Sort(x).
func (x int64Slice) Sort() { sort.Sort(x) }
