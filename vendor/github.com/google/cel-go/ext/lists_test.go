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

package ext

import (
    "fmt"
    "strings"
    "testing"

    "github.com/google/cel-go/cel"
)

func TestLists(t *testing.T) {
    listsTests := []struct {
        expr string
        err  string
    }{
        {expr: `[1,2,3,4].slice(0, 4) == [1,2,3,4]`},
        {expr: `[1,2,3,4].slice(0, 0) == []`},
        {expr: `[1,2,3,4].slice(1, 1) == []`},
        {expr: `[1,2,3,4].slice(4, 4) == []`},
        {expr: `[1,2,3,4].slice(1, 3) == [2, 3]`},
        {expr: `[1,2,3,4].slice(3, 0)`, err: "cannot slice(3, 0), start index must be less than or equal to end index"},
        {expr: `[1,2,3,4].slice(0, 10)`, err: "cannot slice(0, 10), list is length 4"},
        {expr: `[1,2,3,4].slice(-5, 10)`, err: "cannot slice(-5, 10), negative indexes not supported"},
        {expr: `[1,2,3,4].slice(-5, -3)`, err: "cannot slice(-5, -3), negative indexes not supported"},
    }

    env := testListsEnv(t)
    for i, tst := range listsTests {
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
                out, _, err := prg.Eval(cel.NoVars())
                if tc.err != "" {
                    if err == nil {
                        t.Fatalf("got value %v, wanted error %s for expr: %s",
                            out.Value(), tc.err, tc.expr)
                    }
                    if !strings.Contains(err.Error(), tc.err) {
                        t.Errorf("got error %v, wanted error %s for expr: %s", err, tc.err, tc.expr)
                    }
                } else if err != nil {
                    t.Fatal(err)
                } else if out.Value() != true {
                    t.Errorf("got %v, wanted true for expr: %s", out.Value(), tc.expr)
                }
            }
        })
    }
}

func testListsEnv(t *testing.T, opts ...cel.EnvOption) *cel.Env {
    t.Helper()
    baseOpts := []cel.EnvOption{Lists()}
    env, err := cel.NewEnv(append(baseOpts, opts...)...)
    if err != nil {
        t.Fatalf("cel.NewEnv(Lists()) failed: %v", err)
    }
    return env
}
