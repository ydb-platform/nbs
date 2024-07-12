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

package ext

import (
    "fmt"
    "strings"
    "testing"

    "github.com/google/cel-go/cel"
)

func TestMath(t *testing.T) {
    mathTests := []struct {
        expr string
        in   any
    }{
        // Tests for math.least
        {expr: "math.least(-0.5) == -0.5"},
        {expr: "math.least(-1) == -1"},
        {expr: "math.least(1u) == 1u"},
        {expr: "math.least(42.0, -0.5) == -0.5"},
        {expr: "math.least(-1, 0) == -1"},
        {expr: "math.least(-1, -1) == -1"},
        {expr: "math.least(1u, 42u) == 1u"},
        {expr: "math.least(42.0, -0.5, -0.25) == -0.5"},
        {expr: "math.least(-1, 0, 1) == -1"},
        {expr: "math.least(-1, -1, -1) == -1"},
        {expr: "math.least(1u, 42u, 0u) == 0u"},
        // math.least two arg overloads across type.
        {expr: "math.least(1, 1.0) == 1"},
        {expr: "math.least(1, -2.0) == -2.0"},
        {expr: "math.least(2, 1u) == 1u"},
        {expr: "math.least(1.5, 2) == 1.5"},
        {expr: "math.least(1.5, -2) == -2"},
        {expr: "math.least(2.5, 1u) == 1u"},
        {expr: "math.least(1u, 2) == 1u"},
        {expr: "math.least(1u, -2) == -2"},
        {expr: "math.least(2u, 2.5) == 2u"},
        // math.least with dynamic values across type.
        {expr: "math.least(1u, dyn(42)) == 1"},
        {expr: "math.least(1u, dyn(42), dyn(0.0)) == 0u"},
        // math.least with a list literal.
        {expr: "math.least([1u, 42u, 0u]) == 0u"},
        // math.least with expression arguments.
        {
            expr: "math.least(a, b) == a",
            in: map[string]any{
                "a": 1,
                "b": 2,
            },
        },
        {
            expr: "math.least(numbers) == dyn(a)",
            in: map[string]any{
                "a":       -21,
                "numbers": []float64{-21.0, -10.5, 1.0},
            },
        },

        // Tests for math.greatest
        {expr: "math.greatest(-0.5) == -0.5"},
        {expr: "math.greatest(-1) == -1"},
        {expr: "math.greatest(1u) == 1u"},
        {expr: "math.greatest(42.0, -0.5) == 42.0"},
        {expr: "math.greatest(-1, 0) == 0"},
        {expr: "math.greatest(-1, -1) == -1"},
        {expr: "math.greatest(1u, 42u) == 42u"},
        {expr: "math.greatest(42.0, -0.5, -0.25) == 42.0"},
        {expr: "math.greatest(-1, 0, 1) == 1"},
        {expr: "math.greatest(-1, -1, -1) == -1"},
        {expr: "math.greatest(1u, 42u, 0u) == 42u"},
        // math.least two arg overloads across type.
        {expr: "math.greatest(1, 1.0) == 1"},
        {expr: "math.greatest(1, -2.0) == 1"},
        {expr: "math.greatest(2, 1u) == 2"},
        {expr: "math.greatest(1.5, 2) == 2"},
        {expr: "math.greatest(1.5, -2) == 1.5"},
        {expr: "math.greatest(2.5, 1u) == 2.5"},
        {expr: "math.greatest(1u, 2) == 2"},
        {expr: "math.greatest(1u, -2) == 1u"},
        {expr: "math.greatest(2u, 2.5) == 2.5"},
        // math.greatest with dynamic values across type.
        {expr: "math.greatest(1u, dyn(42)) == 42.0"},
        {expr: "math.greatest(1u, dyn(0.0), 0u) == 1"},
        // math.greatest with a list literal
        {expr: "math.greatest([1u, dyn(0.0), 0u]) == 1"},
        // math.greatest with expression arguments.
        {
            expr: "math.greatest(a, b) == b",
            in: map[string]any{
                "a": 1,
                "b": 2,
            },
        },
        {
            expr: "math.greatest(numbers) == dyn(a)",
            in: map[string]any{
                "a":       1,
                "numbers": []float64{-21.0, -10.5, 1.0},
            },
        },
    }

    env := testMathEnv(t,
        cel.Variable("a", cel.IntType),
        cel.Variable("b", cel.IntType),
        cel.Variable("numbers", cel.ListType(cel.DoubleType)),
    )
    for i, tst := range mathTests {
        tc := tst
        t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
            var asts []*cel.Ast
            pAst, iss := env.Parse(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("env.Parse(%v) failed: %v", tc.expr, iss.Err())
            }
            asts = append(asts, pAst)
            cAst, iss := env.Check(pAst)
            if iss.Err() != nil {
                t.Fatalf("env.Check(%v) failed: %v", tc.expr, iss.Err())
            }
            asts = append(asts, cAst)

            for _, ast := range asts {
                prg, err := env.Program(ast)
                if err != nil {
                    t.Fatalf("env.Program() failed: %v", err)
                }
                in := tc.in
                if in == nil {
                    in = cel.NoVars()
                }
                out, _, err := prg.Eval(in)
                if err != nil {
                    t.Fatalf("prg.Eval() failed: %v", err)
                }
                if out.Value() != true {
                    t.Errorf("prg.Eval() got %v, wanted true for expr: %s", out.Value(), tc.expr)
                }
            }
        })
    }
}

func TestMathStaticErrors(t *testing.T) {
    mathTests := []struct {
        expr string
        err  string
    }{
        // Tests for math.least
        {
            expr: "math.least()",
            err:  "math.least() requires at least one argument",
        },
        {
            expr: "math.least('hello')",
            err:  "math.least() invalid single argument value",
        },
        {
            expr: "math.least({})",
            err:  "math.least() invalid single argument value",
        },
        {
            expr: "math.least(1, true)",
            err:  "math.least() simple literal arguments must be numeric",
        },
        {
            expr: "math.least(1, 2, true)",
            err:  "math.least() simple literal arguments must be numeric",
        },

        // Tests for math.greatest
        {
            expr: "math.greatest()",
            err:  "math.greatest() requires at least one argument",
        },
        {
            expr: "math.greatest(true)",
            err:  "math.greatest() invalid single argument value",
        },
        {
            expr: "math.greatest([])",
            err:  "math.greatest() invalid single argument value",
        },
        {
            expr: "math.greatest([1, true])",
            err:  "math.greatest() invalid single argument value",
        },
        {
            expr: "math.greatest(1, true)",
            err:  "math.greatest() simple literal arguments must be numeric",
        },
        {
            expr: "math.greatest(1, 2, true)",
            err:  "math.greatest() simple literal arguments must be numeric",
        },
    }

    env := testMathEnv(t)
    for i, tst := range mathTests {
        tc := tst
        t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
            _, iss := env.Compile(tc.expr)
            if iss.Err() == nil || !strings.Contains(iss.Err().Error(), tc.err) {
                t.Errorf("env.Compile(%q) got %v, wanted error %v", tc.expr, iss.Err(), tc.err)
            }
        })
    }
}

func TestMathRuntimeErrors(t *testing.T) {
    mathTests := []struct {
        expr string
        err  string
        in   any
    }{
        // Tests for math.least
        {
            expr: "math.least(a, b)",
            err:  "no such overload: math.@min",
            in: map[string]any{
                "a": []int{},
                "b": 1,
            },
        },
        {
            expr: "math.least(b, a)",
            err:  "no such overload: math.@min",
            in: map[string]any{
                "a": []int{},
                "b": 1,
            },
        },
        {
            expr: "math.least(a)",
            err:  "math.@min(list) argument must not be empty",
            in: map[string]any{
                "a": []int{},
            },
        },
        {
            expr: "math.least(a)",
            err:  "no such overload: math.@min",
            in: map[string]any{
                "a": []any{"hello"},
            },
        },
        {
            expr: "math.least(a)",
            err:  "no such overload: math.@min",
            in: map[string]any{
                "a": []any{[]int{}, []int{}},
            },
        },
        {
            expr: "math.least(a)",
            err:  "no such overload: math.@min",
            in: map[string]any{
                "a": []any{1, true, 2},
            },
        },
        {
            expr: "math.least(dyn('string'))",
            err:  "no such overload: math.@min",
        },

        // Tests for math.greatest
        {
            expr: "math.greatest(a, b)",
            err:  "no such overload: math.@max",
            in: map[string]any{
                "a": []int{},
                "b": 1,
            },
        },
        {
            expr: "math.greatest(b, a)",
            err:  "no such overload: math.@max",
            in: map[string]any{
                "a": []int{},
                "b": 1,
            },
        },
        {
            expr: "math.greatest(a)",
            err:  "math.@max(list) argument must not be empty",
            in: map[string]any{
                "a": []int{},
            },
        },
        {
            expr: "math.greatest(a)",
            err:  "no such overload: math.@max",
            in: map[string]any{
                "a": []any{true},
            },
        },
        {
            expr: "math.greatest(a)",
            err:  "no such overload: math.@max",
            in: map[string]any{
                "a": []any{1, true, 2},
            },
        },
        {
            expr: "math.greatest(dyn('string'))",
            err:  "no such overload: math.@max",
        },
    }

    env := testMathEnv(t,
        cel.Variable("a", cel.DynType),
        cel.Variable("b", cel.IntType),
        cel.Variable("numbers", cel.ListType(cel.DoubleType)),
    )
    for i, tst := range mathTests {
        tc := tst
        t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
            ast, iss := env.Compile(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("env.Compile(%q) failed with error %v", tc.expr, iss.Err())
            }
            prg, err := env.Program(ast)
            if err != nil {
                t.Fatalf("env.Program(ast) failed: %v", err)
            }
            in := tc.in
            if in == nil {
                in = cel.NoVars()
            }
            _, _, err = prg.Eval(in)
            if err == nil || !strings.Contains(err.Error(), tc.err) {
                t.Errorf("prg.Eval() got %v, wanted %v", err, tc.err)
            }
        })
    }
}

func TestMathNonMatch(t *testing.T) {
    var mathTests = []struct {
        expr string
    }{
        // Even though 'least' is the macro, the call is left unexpanded since the operand is not 'math'.
        {
            expr: `100.least(42) == 42`,
        },
        // Even though 'greatest' is the macro, the call is left unexpanded since the operand is not 'math'.
        {
            expr: `100.greatest(42) == 100`,
        },
    }
    env := testMathEnv(t,
        cel.Function("greatest",
            cel.MemberOverload("int_greatest_int", []*cel.Type{cel.IntType, cel.IntType}, cel.IntType,
                cel.BinaryBinding(maxPair))),
        cel.Function("least",
            cel.MemberOverload("int_least_int", []*cel.Type{cel.IntType, cel.IntType}, cel.IntType,
                cel.BinaryBinding(minPair))),
    )
    for i, tst := range mathTests {
        tc := tst
        t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
            var asts []*cel.Ast
            pAst, iss := env.Parse(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("env.Parse(%v) failed: %v", tc.expr, iss.Err())
            }
            asts = append(asts, pAst)
            cAst, iss := env.Check(pAst)
            if iss.Err() != nil {
                t.Fatalf("env.Check(%v) failed: %v", tc.expr, iss.Err())
            }
            asts = append(asts, cAst)

            for _, ast := range asts {
                prg, err := env.Program(ast)
                if err != nil {
                    t.Fatalf("env.Program() failed: %v", err)
                }
                out, _, err := prg.Eval(cel.NoVars())
                if err != nil {
                    t.Fatalf("prg.Eval() failed: %v", err)
                }
                if out.Value() != true {
                    t.Errorf("prg.Eval() got %v, wanted true for expr: %s", out.Value(), tc.expr)
                }
            }
        })
    }
}

func TestMathWithExtension(t *testing.T) {
    env := testMathEnv(t)
    _, err := env.Extend(Math())
    if err != nil {
        t.Fatalf("env.Extend(Math()) failed: %v", err)
    }
    _, iss := env.Compile("math.least(0, 1, 2) == 0")
    if iss.Err() != nil {
        t.Errorf("env.Compile() failed: %v", iss.Err())
    }
}

func testMathEnv(t *testing.T, opts ...cel.EnvOption) *cel.Env {
    t.Helper()
    baseOpts := []cel.EnvOption{Math(), cel.EnableMacroCallTracking()}
    env, err := cel.NewEnv(append(baseOpts, opts...)...)
    if err != nil {
        t.Fatalf("cel.NewEnv(Math()) failed: %v", err)
    }
    return env
}
