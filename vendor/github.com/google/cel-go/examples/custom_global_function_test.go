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

package examples

import (
    "fmt"
    "log"

    "github.com/google/cel-go/cel"
    "github.com/google/cel-go/common/types"
    "github.com/google/cel-go/common/types/ref"
)

func ExampleCustomGlobalFunction() {
    env, err := cel.NewEnv(
        cel.Variable("i", cel.StringType),
        cel.Variable("you", cel.StringType),
        cel.Function("shake_hands",
            cel.Overload("shake_hands_string_string",
                []*cel.Type{cel.StringType, cel.StringType},
                cel.StringType,
                cel.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
                    return types.String(
                        fmt.Sprintf("%s and %s are shaking hands.\n", lhs, rhs))
                },
                ),
            ),
        ),
    )
    if err != nil {
        log.Fatalf("environment creation error: %v\n", err)
    }
    // Check iss for error in both Parse and Check.
    ast, iss := env.Compile(`shake_hands(i,you)`)
    if iss.Err() != nil {
        log.Fatalln(iss.Err())
    }
    prg, err := env.Program(ast)
    if err != nil {
        log.Fatalf("Program creation error: %v\n", err)
    }

    out, _, err := prg.Eval(map[string]any{
        "i":   "CEL",
        "you": "world",
    })
    if err != nil {
        log.Fatalf("Evaluation error: %v\n", err)
    }

    fmt.Println(out)
    // Output:CEL and world are shaking hands.
}
