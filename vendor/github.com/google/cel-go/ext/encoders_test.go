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

package ext

import (
    "fmt"
    "strings"
    "testing"

    "github.com/google/cel-go/cel"
)

func TestEncoders(t *testing.T) {
    var tests = []struct {
        expr      string
        err       string
        parseOnly bool
    }{
        {expr: "base64.decode('aGVsbG8=') == b'hello'"},
        {
            expr: "base64.decode('aGVsbG8') == b'error'",
            err:  "illegal base64 data at input byte 4",
        },
        {
            expr:      "base64.decode(b'aGVsbG8=') == b'hello'",
            err:       "no such overload",
            parseOnly: true,
        },
        {expr: "base64.encode(b'hello') == 'aGVsbG8='"},
        {
            expr:      "base64.encode('hello') == b'aGVsbG8='",
            err:       "no such overload",
            parseOnly: true,
        },
    }

    env, err := cel.NewEnv(Encoders())
    if err != nil {
        t.Fatalf("cel.NewEnv(Encoders()) failed: %v", err)
    }
    for i, tst := range tests {
        tc := tst
        t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
            var asts []*cel.Ast
            pAst, iss := env.Parse(tc.expr)
            if iss.Err() != nil {
                t.Fatalf("env.Parse(%v) failed: %v", tc.expr, iss.Err())
            }
            asts = append(asts, pAst)
            if !tc.parseOnly {
                cAst, iss := env.Check(pAst)
                if iss.Err() != nil {
                    t.Fatalf("env.Check(%v) failed: %v", tc.expr, iss.Err())
                }
                asts = append(asts, cAst)
            }
            for _, ast := range asts {
                prg, err := env.Program(ast)
                if err != nil {
                    t.Fatal(err)
                }
                out, _, err := prg.Eval(cel.NoVars())
                if tc.err != "" {
                    if err == nil {
                        t.Fatalf("got %v, wanted error %s for expr: %s",
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
