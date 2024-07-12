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

func ExampleCustomInstanceFunction() {
    env, err := cel.NewEnv(cel.Lib(customLib{}))
    if err != nil {
        log.Fatalf("environment creation error: %v\n", err)
    }
    // Check iss for error in both Parse and Check.
    ast, iss := env.Compile(`i.greet(you)`)
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
    // Output:Hello world! Nice to meet you, I'm CEL.
}

type customLib struct{}

func (customLib) CompileOptions() []cel.EnvOption {
    return []cel.EnvOption{
        cel.Variable("i", cel.StringType),
        cel.Variable("you", cel.StringType),
        cel.Function("greet",
            cel.MemberOverload("string_greet_string",
                []*cel.Type{cel.StringType, cel.StringType},
                cel.StringType,
                cel.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
                    return types.String(
                        fmt.Sprintf("Hello %s! Nice to meet you, I'm %s.\n", rhs, lhs))
                }),
            ),
        ),
    }
}

func (customLib) ProgramOptions() []cel.ProgramOption {
    return []cel.ProgramOption{}
}
