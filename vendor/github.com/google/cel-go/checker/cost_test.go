// Copyright 2022 Google LLC
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

package checker

import (
    "math"
    "strings"
    "testing"

    "github.com/google/cel-go/common"
    "github.com/google/cel-go/common/containers"
    "github.com/google/cel-go/common/decls"
    "github.com/google/cel-go/common/overloads"
    "github.com/google/cel-go/common/stdlib"
    "github.com/google/cel-go/common/types"
    "github.com/google/cel-go/parser"

    proto3pb "github.com/google/cel-go/test/proto3pb"
)

func TestCost(t *testing.T) {
    allTypes := types.NewObjectType("google.expr.proto3.test.TestAllTypes")
    allList := types.NewListType(allTypes)
    intList := types.NewListType(types.IntType)
    nestedList := types.NewListType(allList)

    allMap := types.NewMapType(types.StringType, allTypes)
    nestedMap := types.NewMapType(types.StringType, allMap)

    zeroCost := CostEstimate{}
    oneCost := CostEstimate{Min: 1, Max: 1}
    cases := []struct {
        name    string
        expr    string
        vars    []*decls.VariableDecl
        hints   map[string]uint64
        options []CostOption
        wanted  CostEstimate
    }{
        {
            name:   "const",
            expr:   `"Hello World!"`,
            wanted: zeroCost,
        },
        {
            name:   "identity",
            expr:   `input`,
            vars:   []*decls.VariableDecl{decls.NewVariable("input", intList)},
            wanted: CostEstimate{Min: 1, Max: 1},
        },
        {
            name: "select: map",
            expr: `input['key']`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", types.NewMapType(types.StringType, types.StringType))},
            wanted: CostEstimate{Min: 2, Max: 2},
        },
        {
            name:   "select: field",
            expr:   `input.single_int32`,
            vars:   []*decls.VariableDecl{decls.NewVariable("input", allTypes)},
            wanted: CostEstimate{Min: 2, Max: 2},
        },
        {
            name:    "select: field test only no has() cost",
            expr:    `has(input.single_int32)`,
            vars:    []*decls.VariableDecl{decls.NewVariable("input", types.NewObjectType("google.expr.proto3.test.TestAllTypes"))},
            wanted:  CostEstimate{Min: 1, Max: 1},
            options: []CostOption{PresenceTestHasCost(false)},
        },
        {
            name:   "select: field test only",
            expr:   `has(input.single_int32)`,
            vars:   []*decls.VariableDecl{decls.NewVariable("input", types.NewObjectType("google.expr.proto3.test.TestAllTypes"))},
            wanted: CostEstimate{Min: 2, Max: 2},
        },
        {
            name:    "select: non-proto field test has() cost",
            expr:    `has(input.testAttr.nestedAttr)`,
            vars:    []*decls.VariableDecl{decls.NewVariable("input", nestedMap)},
            wanted:  CostEstimate{Min: 3, Max: 3},
            options: []CostOption{PresenceTestHasCost(true)},
        },
        {
            name:    "select: non-proto field test no has() cost",
            expr:    `has(input.testAttr.nestedAttr)`,
            vars:    []*decls.VariableDecl{decls.NewVariable("input", nestedMap)},
            wanted:  CostEstimate{Min: 2, Max: 2},
            options: []CostOption{PresenceTestHasCost(false)},
        },
        {
            name:   "select: non-proto field test",
            expr:   `has(input.testAttr.nestedAttr)`,
            vars:   []*decls.VariableDecl{decls.NewVariable("input", nestedMap)},
            wanted: CostEstimate{Min: 3, Max: 3},
        },
        {
            name:   "estimated function call",
            expr:   `input.getFullYear()`,
            vars:   []*decls.VariableDecl{decls.NewVariable("input", types.TimestampType)},
            wanted: CostEstimate{Min: 8, Max: 8},
        },
        {
            name:   "create list",
            expr:   `[1, 2, 3]`,
            wanted: CostEstimate{Min: 10, Max: 10},
        },
        {
            name:   "create struct",
            expr:   `google.expr.proto3.test.TestAllTypes{single_int32: 1, single_float: 3.14, single_string: 'str'}`,
            wanted: CostEstimate{Min: 40, Max: 40},
        },
        {
            name:   "create map",
            expr:   `{"a": 1, "b": 2, "c": 3}`,
            wanted: CostEstimate{Min: 30, Max: 30},
        },
        {
            name:   "all comprehension",
            vars:   []*decls.VariableDecl{decls.NewVariable("input", allList)},
            hints:  map[string]uint64{"input": 100},
            expr:   `input.all(x, true)`,
            wanted: CostEstimate{Min: 2, Max: 302},
        },
        {
            name:   "nested all comprehension",
            vars:   []*decls.VariableDecl{decls.NewVariable("input", nestedList)},
            hints:  map[string]uint64{"input": 50, "input.@items": 10},
            expr:   `input.all(x, x.all(y, true))`,
            wanted: CostEstimate{Min: 2, Max: 1752},
        },
        {
            name:   "all comprehension on literal",
            expr:   `[1, 2, 3].all(x, true)`,
            wanted: CostEstimate{Min: 20, Max: 20},
        },
        {
            name:   "variable cost function",
            vars:   []*decls.VariableDecl{decls.NewVariable("input", types.StringType)},
            hints:  map[string]uint64{"input": 500},
            expr:   `input.matches('[0-9]')`,
            wanted: CostEstimate{Min: 3, Max: 103},
        },
        {
            name:   "variable cost function with constant",
            expr:   `'123'.matches('[0-9]')`,
            wanted: CostEstimate{Min: 2, Max: 2},
        },
        {
            name:   "or",
            expr:   `true || false`,
            wanted: zeroCost,
        },
        {
            name: "or accumulated branch cost",
            expr: `a || b || c || d`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("a", types.BoolType),
                decls.NewVariable("b", types.BoolType),
                decls.NewVariable("c", types.BoolType),
                decls.NewVariable("d", types.BoolType),
            },
            wanted: CostEstimate{Min: 1, Max: 4},
        },
        {
            name:   "and",
            expr:   `true && false`,
            wanted: zeroCost,
        },
        {
            name: "and accumulated branch cost",
            expr: `a && b && c && d`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("a", types.BoolType),
                decls.NewVariable("b", types.BoolType),
                decls.NewVariable("c", types.BoolType),
                decls.NewVariable("d", types.BoolType),
            },
            wanted: CostEstimate{Min: 1, Max: 4},
        },
        {
            name:   "lt",
            expr:   `1 < 2`,
            wanted: oneCost,
        },
        {
            name:   "lte",
            expr:   `1 <= 2`,
            wanted: oneCost,
        },
        {
            name:   "eq",
            expr:   `1 == 2`,
            wanted: oneCost,
        },
        {
            name:   "gt",
            expr:   `2 > 1`,
            wanted: oneCost,
        },
        {
            name:   "gte",
            expr:   `2 >= 1`,
            wanted: oneCost,
        },
        {
            name:   "in",
            expr:   `2 in [1, 2, 3]`,
            wanted: CostEstimate{Min: 13, Max: 13},
        },
        {
            name:   "plus",
            expr:   `1 + 1`,
            wanted: oneCost,
        },
        {
            name:   "minus",
            expr:   `1 - 1`,
            wanted: oneCost,
        },
        {
            name:   "/",
            expr:   `1 / 1`,
            wanted: oneCost,
        },
        {
            name:   "/",
            expr:   `1 * 1`,
            wanted: oneCost,
        },
        {
            name:   "%",
            expr:   `1 % 1`,
            wanted: oneCost,
        },
        {
            name:   "ternary",
            expr:   `true ? 1 : 2`,
            wanted: zeroCost,
        },
        {
            name:   "string size",
            expr:   `size("123")`,
            wanted: oneCost,
        },
        {
            name:   "bytes to string conversion",
            vars:   []*decls.VariableDecl{decls.NewVariable("input", types.BytesType)},
            hints:  map[string]uint64{"input": 500},
            expr:   `string(input)`,
            wanted: CostEstimate{Min: 1, Max: 51},
        },
        {
            name:  "bytes to string conversion equality",
            vars:  []*decls.VariableDecl{decls.NewVariable("input", types.BytesType)},
            hints: map[string]uint64{"input": 500},
            // equality check ensures that the resultSize calculation is included in cost
            expr:   `string(input) == string(input)`,
            wanted: CostEstimate{Min: 3, Max: 152},
        },
        {
            name:   "string to bytes conversion",
            vars:   []*decls.VariableDecl{decls.NewVariable("input", types.StringType)},
            hints:  map[string]uint64{"input": 500},
            expr:   `bytes(input)`,
            wanted: CostEstimate{Min: 1, Max: 51},
        },
        {
            name:  "string to bytes conversion equality",
            vars:  []*decls.VariableDecl{decls.NewVariable("input", types.StringType)},
            hints: map[string]uint64{"input": 500},
            // equality check ensures that the resultSize calculation is included in cost
            expr:   `bytes(input) == bytes(input)`,
            wanted: CostEstimate{Min: 3, Max: 302},
        },
        {
            name:   "int to string conversion",
            expr:   `string(1)`,
            wanted: CostEstimate{Min: 1, Max: 1},
        },
        {
            name: "contains",
            expr: `input.contains(arg1)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", types.StringType),
                decls.NewVariable("arg1", types.StringType),
            },
            hints:  map[string]uint64{"input": 500, "arg1": 500},
            wanted: CostEstimate{Min: 2, Max: 2502},
        },
        {
            name: "matches",
            expr: `input.matches('\\d+a\\d+b')`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", types.StringType),
            },
            hints:  map[string]uint64{"input": 500},
            wanted: CostEstimate{Min: 3, Max: 103},
        },
        {
            name: "startsWith",
            expr: `input.startsWith(arg1)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", types.StringType),
                decls.NewVariable("arg1", types.StringType),
            },
            hints:  map[string]uint64{"arg1": 500},
            wanted: CostEstimate{Min: 2, Max: 52},
        },
        {
            name: "endsWith",
            expr: `input.endsWith(arg1)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", types.StringType),
                decls.NewVariable("arg1", types.StringType),
            },
            hints:  map[string]uint64{"arg1": 500},
            wanted: CostEstimate{Min: 2, Max: 52},
        },
        {
            name: "size receiver",
            expr: `input.size()`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", types.StringType),
            },
            wanted: CostEstimate{Min: 2, Max: 2},
        },
        {
            name: "size",
            expr: `size(input)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", types.StringType),
            },
            wanted: CostEstimate{Min: 2, Max: 2},
        },
        {
            name: "ternary eval",
            expr: `(x > 2 ? input1 : input2).all(y, true)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("x", types.IntType),
                decls.NewVariable("input1", allList),
                decls.NewVariable("input2", allList),
            },
            hints:  map[string]uint64{"input1": 1, "input2": 1},
            wanted: CostEstimate{Min: 4, Max: 7},
        },
        {
            name: "comprehension over map",
            expr: `input.all(k, input[k].single_int32 > 3)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", allMap),
            },
            hints:  map[string]uint64{"input": 10},
            wanted: CostEstimate{Min: 2, Max: 82},
        },
        {
            name: "comprehension over nested map of maps",
            expr: `input.all(k, input[k].all(x, true))`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", nestedMap),
            },
            hints:  map[string]uint64{"input": 5, "input.@values": 10},
            wanted: CostEstimate{Min: 2, Max: 187},
        },
        {
            name: "string size of map keys",
            expr: `input.all(k, k.contains(k))`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", nestedMap),
            },
            hints:  map[string]uint64{"input": 5, "input.@keys": 10},
            wanted: CostEstimate{Min: 2, Max: 32},
        },
        {
            name: "comprehension variable shadowing",
            expr: `input.all(k, input[k].all(k, true) && k.contains(k))`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", nestedMap),
            },
            hints:  map[string]uint64{"input": 2, "input.@values": 2, "input.@keys": 5},
            wanted: CostEstimate{Min: 2, Max: 34},
        },
        {
            name: "comprehension variable shadowing",
            expr: `input.all(k, input[k].all(k, true) && k.contains(k))`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("input", nestedMap),
            },
            hints:  map[string]uint64{"input": 2, "input.@values": 2, "input.@keys": 5},
            wanted: CostEstimate{Min: 2, Max: 34},
        },
        {
            name: "list concat",
            expr: `(list1 + list2).all(x, true)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("list1", types.NewListType(types.IntType)),
                decls.NewVariable("list2", types.NewListType(types.IntType)),
            },
            hints:  map[string]uint64{"list1": 10, "list2": 10},
            wanted: CostEstimate{Min: 4, Max: 64},
        },
        {
            name: "str concat",
            expr: `"abcdefg".contains(str1 + str2)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("str1", types.StringType),
                decls.NewVariable("str2", types.StringType),
            },
            hints:  map[string]uint64{"str1": 10, "str2": 10},
            wanted: CostEstimate{Min: 2, Max: 6},
        },
        {
            name: "str concat custom cost estimate",
            expr: `"abcdefg".contains(str1 + str2)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("str1", types.StringType),
                decls.NewVariable("str2", types.StringType),
            },
            hints: map[string]uint64{"str1": 10, "str2": 10},
            options: []CostOption{
                OverloadCostEstimate(overloads.ContainsString,
                    func(estimator CostEstimator, target *AstNode, args []AstNode) *CallEstimate {
                        if target != nil && len(args) == 1 {
                            strSize := estimateSize(estimator, *target).MultiplyByCostFactor(0.2)
                            subSize := estimateSize(estimator, args[0]).MultiplyByCostFactor(0.2)
                            return &CallEstimate{CostEstimate: strSize.Multiply(subSize)}
                        }
                        return nil
                    }),
            },
            wanted: CostEstimate{Min: 2, Max: 12},
        },
        {
            name: "list size comparison",
            expr: `list1.size() == list2.size()`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("list1", types.NewListType(types.IntType)),
                decls.NewVariable("list2", types.NewListType(types.IntType)),
            },
            wanted: CostEstimate{Min: 5, Max: 5},
        },
        {
            name: "list size from ternary",
            expr: `x > y ? list1.size() : list2.size()`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("x", types.IntType),
                decls.NewVariable("y", types.IntType),
                decls.NewVariable("list1", types.NewListType(types.IntType)),
                decls.NewVariable("list2", types.NewListType(types.IntType)),
            },
            wanted: CostEstimate{Min: 5, Max: 5},
        },
        {
            name: "str endsWith equality",
            expr: `str1.endsWith("abcdefghijklmnopqrstuvwxyz") == str2.endsWith("abcdefghijklmnopqrstuvwxyz")`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("str1", types.StringType),
                decls.NewVariable("str2", types.StringType),
            },
            wanted: CostEstimate{Min: 9, Max: 9},
        },
        {
            name:   "nested subexpression operators",
            expr:   `((5 != 6) == (1 == 2)) == ((3 <= 4) == (9 != 9))`,
            wanted: CostEstimate{Min: 7, Max: 7},
        },
        {
            name: "str size estimate",
            expr: `string(timestamp1) == string(timestamp2)`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("timestamp1", types.TimestampType),
                decls.NewVariable("timestamp2", types.TimestampType),
            },
            wanted: CostEstimate{Min: 5, Max: 1844674407370955268},
        },
        {
            name: "timestamp equality check",
            expr: `timestamp1 == timestamp2`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("timestamp1", types.TimestampType),
                decls.NewVariable("timestamp2", types.TimestampType),
            },
            wanted: CostEstimate{Min: 3, Max: 3},
        },
        {
            name: "duration inequality check",
            expr: `duration1 != duration2`,
            vars: []*decls.VariableDecl{
                decls.NewVariable("duration1", types.DurationType),
                decls.NewVariable("duration2", types.DurationType),
            },
            wanted: CostEstimate{Min: 3, Max: 3},
        },
        {
            name:   ".filter list literal",
            expr:   `[1,2,3,4,5].filter(x, x % 2 == 0)`,
            wanted: CostEstimate{Min: 41, Max: 101},
        },
        {
            name:   ".map list literal",
            expr:   `[1,2,3,4,5].map(x, x)`,
            wanted: CostEstimate{Min: 86, Max: 86},
        },
        {
            name:   ".map.filter list literal",
            expr:   `[1,2,3,4,5].map(x, x).filter(x, x % 2 == 0)`,
            wanted: CostEstimate{Min: 117, Max: 177},
        },
        {
            name:   ".map.exists list literal",
            expr:   `[1,2,3,4,5].map(x, x).exists(x, x == 5) == true`,
            wanted: CostEstimate{Min: 108, Max: 118},
        },
        {
            name:   ".map.map list literal",
            expr:   `[1,2,3,4,5].map(x, x).map(x, x)`,
            wanted: CostEstimate{Min: 162, Max: 162},
        },
        {
            name:   ".map list literal selection",
            expr:   `[1,2,3,4,5].map(x, x)[4]`,
            wanted: CostEstimate{Min: 87, Max: 87},
        },
        {
            name:   "nested array selection",
            expr:   `[[1,2],[1,2],[1,2],[1,2],[1,2]][4]`,
            wanted: CostEstimate{Min: 61, Max: 61},
        },
        {
            name:   "nested array selection",
            expr:   `{'a': [1,2], 'b': [1,2], 'c': [1,2], 'd': [1,2], 'e': [1,2]}.b`,
            wanted: CostEstimate{Min: 81, Max: 81},
        },
        {
            // Estimated cost does not track the sizes of nested aggregate types
            // (lists, maps, ...) and so assumes a worst case cost when an
            // expression applies a comprehension to a nested aggregated type,
            // even if the size information is available.
            // TODO: This should be fixed.
            name:   "comprehension on nested list",
            expr:   `[1,2,3,4,5].map(x, [x, x]).all(y, y.all(y, y == 1))`,
            wanted: CostEstimate{Min: 157, Max: 18446744073709551615},
        },
        {
            // Make sure we're accounting for not just the iteration range size,
            // but also the overall comprehension size. The chained map calls
            // will treat the result of one map as the iteration range of the other,
            // so they're planned in reverse; however, the `+` should verify that
            // the comprehension result has a size.
            name:   "comprehension size",
            expr:   `[1,2,3,4,5].map(x, x).map(x, x) + [1]`,
            wanted: CostEstimate{Min: 173, Max: 173},
        },
        {
            name:   "nested comprehension",
            expr:   `[1,2,3].all(i, i in [1,2,3].map(j, j + j))`,
            wanted: CostEstimate{Min: 20, Max: 230},
        },
    }

    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            if tc.hints == nil {
                tc.hints = map[string]uint64{}
            }
            p, err := parser.NewParser(parser.Macros(parser.AllMacros...))
            if err != nil {
                t.Fatalf("parser.NewParser() failed: %v", err)
            }
            src := common.NewStringSource(tc.expr, "<input>")
            pe, errs := p.Parse(src)
            if len(errs.GetErrors()) != 0 {
                t.Fatalf("parser.Parse(%v) failed: %v", tc.expr, errs.ToDisplayString())
            }
            reg, err := types.NewRegistry(&proto3pb.TestAllTypes{})
            if err != nil {
                t.Fatalf("types.NewRegistry(...) failed: %v", err)
            }

            e, err := NewEnv(containers.DefaultContainer, reg)
            if err != nil {
                t.Fatalf("NewEnv() failed: %v", err)
            }
            err = e.AddFunctions(stdlib.Functions()...)
            if err != nil {
                t.Fatalf("environment creation error: %v", err)
            }
            err = e.AddIdents(tc.vars...)
            if err != nil {
                t.Fatalf("environment creation error: %s\n", err)
            }
            checked, errs := Check(pe, src, e)
            if len(errs.GetErrors()) != 0 {
                t.Fatalf("Check(%s) failed: %v", tc.expr, errs.ToDisplayString())
            }
            est, err := Cost(checked, testCostEstimator{hints: tc.hints}, tc.options...)
            if err != nil {
                t.Fatalf("Cost() failed: %v", err)
            }
            if est.Min != tc.wanted.Min || est.Max != tc.wanted.Max {
                t.Fatalf("Got cost interval [%v, %v], wanted [%v, %v]",
                    est.Min, est.Max, tc.wanted.Min, tc.wanted.Max)
            }
        })
    }
}

type testCostEstimator struct {
    hints map[string]uint64
}

func (tc testCostEstimator) EstimateSize(element AstNode) *SizeEstimate {
    if l, ok := tc.hints[strings.Join(element.Path(), ".")]; ok {
        return &SizeEstimate{Min: 0, Max: l}
    }
    return nil
}

func (tc testCostEstimator) EstimateCallCost(function, overloadID string, target *AstNode, args []AstNode) *CallEstimate {
    switch overloadID {
    case overloads.TimestampToYear:
        return &CallEstimate{CostEstimate: CostEstimate{Min: 7, Max: 7}}
    }
    return nil
}

func estimateSize(estimator CostEstimator, node AstNode) SizeEstimate {
    if l := node.ComputedSize(); l != nil {
        return *l
    }
    if l := estimator.EstimateSize(node); l != nil {
        return *l
    }
    return SizeEstimate{Min: 0, Max: math.MaxUint64}
}
