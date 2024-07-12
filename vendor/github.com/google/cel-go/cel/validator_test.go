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
    "testing"

    "github.com/google/cel-go/common/operators"
    "github.com/google/cel-go/common/overloads"
    "github.com/google/cel-go/common/types"
    "github.com/google/cel-go/common/types/ref"
    "github.com/google/cel-go/common/types/traits"
    "github.com/google/cel-go/test"
)

func TestValidateDurationLiterals(t *testing.T) {
    env, err := NewEnv(
        Variable("x", types.StringType),
        ASTValidators(ValidateDurationLiterals()))
    if err != nil {
        t.Fatalf("NewEnv(ValidateDurationLiterals()) failed: %v", err)
    }

    tests := []struct {
        expr string
        iss  string
    }{
        {
            expr: `duration('1')`,
            iss: `ERROR: <input>:1:10: invalid duration argument
            | duration('1')
            | .........^`,
        },
        {
            expr: `duration('1d')`,
            iss: `ERROR: <input>:1:10: invalid duration argument
            | duration('1d')
            | .........^`,
        },
        {
            expr: "duration('1us')\n < duration('1nns')",
            iss: `ERROR: <input>:2:13: invalid duration argument
            |  < duration('1nns')
            | ............^`,
        },
        {
            expr: `duration('2h3m4s5us')`,
        },
        {
            expr: `duration(x)`,
        },
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            _, iss := env.Compile(tc.expr)
            if tc.iss != "" {
                if iss.Err() == nil {
                    t.Fatalf("e.Compile(%v) returned ast, expected error: %v", tc.expr, tc.iss)
                }
                if !test.Compare(iss.Err().Error(), tc.iss) {
                    t.Fatalf("e.Compile(%v) returned %v, expected error: %v", tc.expr, iss.Err(), tc.iss)
                }
                return
            }
            if iss.Err() != nil {
                t.Fatalf("e.Compile(%v) failed: %v", tc.expr, iss.Err())
            }
        })
    }
}

func TestValidateTimestampLiterals(t *testing.T) {
    env, err := NewEnv(
        Variable("x", types.StringType),
        ASTValidators(ValidateTimestampLiterals()))
    if err != nil {
        t.Fatalf("NewEnv(ValidateTimestampLiterals()) failed: %v", err)
    }

    tests := []struct {
        expr string
        iss  string
    }{
        {
            expr: `timestamp('1000-00-00T00:00:00Z')`,
            iss: `ERROR: <input>:1:11: invalid timestamp argument
            | timestamp('1000-00-00T00:00:00Z')
            | ..........^`,
        },
        {
            expr: `timestamp('1000-01-01T00:00:00ZZ')`,
            iss: `ERROR: <input>:1:11: invalid timestamp argument
            | timestamp('1000-01-01T00:00:00ZZ')
            | ..........^`,
        },
        {
            expr: `timestamp('1000-01-01T00:00:00Z')`,
        },
        {
            expr: `timestamp(-6213559680)`, // min unix epoch time.
        },
        {
            expr: `timestamp(-62135596801)`,
            iss: `ERROR: <input>:1:11: invalid timestamp argument
            | timestamp(-62135596801)
            | ..........^`,
        },
        {
            expr: `timestamp(x)`,
        },
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            _, iss := env.Compile(tc.expr)
            if tc.iss != "" {
                if iss.Err() == nil {
                    t.Fatalf("e.Compile(%v) returned ast, expected error: %v", tc.expr, tc.iss)
                }
                if !test.Compare(iss.Err().Error(), tc.iss) {
                    t.Fatalf("e.Compile(%v) returned %v, expected error: %v", tc.expr, iss.Err(), tc.iss)
                }
                return
            }
            if iss.Err() != nil {
                t.Fatalf("e.Compile(%v) failed: %v", tc.expr, iss.Err())
            }
        })
    }
}

func TestValidateRegexLiterals(t *testing.T) {
    env, err := NewEnv(
        Variable("x", types.StringType),
        ASTValidators(ValidateRegexLiterals()))
    if err != nil {
        t.Fatalf("NewEnv(ValidateRegexLiterals()) failed: %v", err)
    }

    tests := []struct {
        expr string
        iss  string
    }{
        {
            expr: `'hello'.matches('el*')`,
        },
        {
            expr: `'hello'.matches('x++')`,
            iss: `
            ERROR: <input>:1:17: invalid matches argument
             | 'hello'.matches('x++')
             | ................^`,
        },
        {
            expr: `'hello'.matches('(?<name%>el*)')`,
            iss: `
            ERROR: <input>:1:17: invalid matches argument
             | 'hello'.matches('(?<name%>el*)')
             | ................^`,
        },
        {
            expr: `'hello'.matches('??el*')`,
            iss: `
            ERROR: <input>:1:17: invalid matches argument
             | 'hello'.matches('??el*')
             | ................^`,
        },
        {
            expr: `'hello'.matches(x)`,
        },
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            _, iss := env.Compile(tc.expr)
            if tc.iss != "" {
                if iss.Err() == nil {
                    t.Fatalf("e.Compile(%v) returned ast, expected error: %v", tc.expr, tc.iss)
                }
                if !test.Compare(iss.Err().Error(), tc.iss) {
                    t.Fatalf("e.Compile(%v) returned %v, expected error: %v", tc.expr, iss.Err(), tc.iss)
                }
                return
            }
            if iss.Err() != nil {
                t.Fatalf("e.Compile(%v) failed: %v", tc.expr, iss.Err())
            }
        })
    }
}

func TestValidateHomogeneousAggregateLiterals(t *testing.T) {
    env, err := NewCustomEnv(
        Variable("name", StringType),
        Function(operators.In,
            Overload(overloads.InList, []*Type{StringType, ListType(StringType)}, BoolType,
                BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
                    return rhs.(traits.Container).Contains(lhs)
                }),
            ),
            Overload(overloads.InMap, []*Type{StringType, MapType(StringType, BoolType)}, BoolType,
                BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
                    return rhs.(traits.Container).Contains(lhs)
                }),
            ),
        ),
        OptionalTypes(),
        HomogeneousAggregateLiterals(),
        ASTValidators(ValidateHomogeneousAggregateLiterals()),
    )
    if err != nil {
        t.Fatalf("NewCustomEnv() failed: %v", err)
    }

    tests := []struct {
        expr string
        iss  string
    }{
        {
            expr: `name in ['hello', 0]`,
            iss: `
            ERROR: <input>:1:19: expected type 'string' but found 'int'
             | name in ['hello', 0]
             | ..................^`,
        },
        {
            expr: `{'hello':'world', 1:'!'}`,
            iss: `
            ERROR: <input>:1:19: expected type 'string' but found 'int'
             | {'hello':'world', 1:'!'}
             | ..................^`,
        },
        {
            expr: `name in {'hello':'world', 'goodbye':true}`,
            iss: `
            ERROR: <input>:1:37: expected type 'string' but found 'bool'
             | name in {'hello':'world', 'goodbye':true}
             | ....................................^`,
        },
        {
            expr: `name in ['hello', 'world']`,
        },
        {
            expr: `name in ['hello', ?optional.ofNonZeroValue('')]`,
        },
        {
            expr: `name in [?optional.ofNonZeroValue(''), 'hello', ?optional.of('')]`,
        },
        {
            expr: `name in {'hello': false, 'world': true}`,
        },
        {
            expr: `{'hello': false, ?'world': optional.ofNonZeroValue(true)}`,
        },
        {
            expr: `{?'hello': optional.ofNonZeroValue(false), 'world': true}`,
        },
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            _, iss := env.Compile(tc.expr)
            if tc.iss != "" {
                if iss.Err() == nil {
                    t.Fatalf("e.Compile(%v) returned ast, expected error: %v", tc.expr, tc.iss)
                }
                if !test.Compare(iss.Err().Error(), tc.iss) {
                    t.Fatalf("e.Compile(%v) returned %v, expected error: %v", tc.expr, iss.Err(), tc.iss)
                }
                return
            }
            if iss.Err() != nil {
                t.Fatalf("e.Compile(%v) failed: %v", tc.expr, iss.Err())
            }
        })
    }
}

func TestValidateComprehensionNestingLimit(t *testing.T) {
    env, err := NewEnv(
        ASTValidators(ValidateComprehensionNestingLimit(2)),
    )
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    tests := []struct {
        expr string
        iss  string
    }{
        {
            expr: `[1, 2, 3].exists(i, i < 1)`,
        },
        {
            expr: `[1, 2, 3].exists(i, [4, 5, 6].filter(j, j % i != 0).size() > 0)`,
        },
        {
            // three comprehensions, but not three levels deep
            expr: `[1, 2, 3].exists(i, [4, 5, 6].filter(j, j % i != 0).size() > 0) && [1, 2, 3].exists(i, i < 1)`,
        },
        {
            // the empty iteration range in [].all(k, k) does not impact the actual runtime complexity,
            // so it does not trip the comprehension limit.
            expr: `[1, 2, 3].exists(i, [4, 5, 6].filter(j, [].all(k, k) && j % i != 0).size() > 0)`,
        },
        {
            // three comprehensions, three levels deep
            expr: `[1, 2, 3].map(i, [4, 5, 6].map(j, [7, 8, 9].map(k, i * j * k)))`,
            iss: `
            ERROR: <input>:1:48: comprehension exceeds nesting limit
             | [1, 2, 3].map(i, [4, 5, 6].map(j, [7, 8, 9].map(k, i * j * k)))
             | ...............................................^`,
        },
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            _, iss := env.Compile(tc.expr)
            if tc.iss != "" {
                if iss.Err() == nil {
                    t.Fatalf("e.Compile(%v) returned ast, expected error: %v", tc.expr, tc.iss)
                }
                if !test.Compare(iss.Err().Error(), tc.iss) {
                    t.Fatalf("e.Compile(%v) returned %v, expected error: %v", tc.expr, iss.Err(), tc.iss)
                }
                return
            }
            if iss.Err() != nil {
                t.Fatalf("e.Compile(%v) failed: %v", tc.expr, iss.Err())
            }
        })
    }
}

func TestExtendedValidations(t *testing.T) {
    env, err := NewEnv(
        Variable("x", types.StringType),
        ExtendedValidations(),
    )
    if err != nil {
        t.Fatalf("NewEnv(ExtendedValidations()) failed: %v", err)
    }
    tests := []struct {
        expr string
        iss  string
    }{
        {
            expr: `x in ['hello', 0]
            && duration(x) < duration('1d')
            && timestamp(x) != timestamp('1000-01-00T00:00:00Z')
            && x.matches('x++')`,
            iss: `
            ERROR: <input>:1:16: expected type 'string' but found 'int'
             | x in ['hello', 0]
             | ...............^
            ERROR: <input>:2:30: invalid duration argument
             |    && duration(x) < duration('1d')
             | .............................^
            ERROR: <input>:3:33: invalid timestamp argument
             |    && timestamp(x) != timestamp('1000-01-00T00:00:00Z')
             | ................................^
            ERROR: <input>:4:17: invalid matches argument
             |    && x.matches('x++')
             | ................^`,
        },
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.expr, func(t *testing.T) {
            _, iss := env.Compile(tc.expr)
            if tc.iss != "" {
                if iss.Err() == nil {
                    t.Fatalf("e.Compile(%v) returned ast, expected error: %v", tc.expr, tc.iss)
                }
                if !test.Compare(iss.Err().Error(), tc.iss) {
                    t.Fatalf("e.Compile(%v) returned %v, expected error: %v", tc.expr, iss.Err(), tc.iss)
                }
                return
            }
            if iss.Err() != nil {
                t.Fatalf("e.Compile(%v) failed: %v", tc.expr, iss.Err())
            }
        })
    }
}

func TestValidatorConfig(t *testing.T) {
    config := newValidatorConfig()
    result := config.GetOrDefault("ext.validate.custom", 2)
    if result != 2 {
        t.Errorf("config.GetOrDefault() got %v, wanted default of 2", result)
    }
    result = config.GetOrDefault(HomogeneousAggregateLiteralExemptFunctions, []string{})
    if reflect.TypeOf(result) != reflect.TypeOf([]string{}) {
        t.Errorf("config.GetOrDefault() got %T, wanted type %T", result, []string{})
    }
    err := config.Set(HomogeneousAggregateLiteralExemptFunctions, []string{"_==_"})
    if err != nil {
        t.Errorf("config.Set() failed: %v", err)
    }
    err = config.Set(HomogeneousAggregateLiteralExemptFunctions, map[string]any{})
    if err == nil {
        t.Error("config.Set() with incorrect value type did not fail")
    }
}
