// Copyright 2021 Google LLC
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
    "fmt"
    "reflect"
    "sync"
    "testing"

    "github.com/google/cel-go/common"
    "github.com/google/cel-go/common/operators"
    "github.com/google/cel-go/common/types"
    "github.com/google/cel-go/common/types/ref"

    proto3pb "github.com/google/cel-go/test/proto3pb"
    exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func TestAstNil(t *testing.T) {
    var ast *Ast
    if ast.IsChecked() {
        t.Error("ast.IsChecked() returned true for nil ast")
    }
    if ast.Expr() != nil {
        t.Errorf("ast.Expr() got %v, wanted nil", ast.Expr())
    }
    if ast.SourceInfo() != nil {
        t.Errorf("ast.SourceInfo() got %v, wanted nil", ast.SourceInfo())
    }
    if ast.OutputType() != types.ErrorType {
        t.Errorf("ast.OutputType() got %v, wanted error type", ast.OutputType())
    }
    if ast.Source() != nil {
        t.Errorf("ast.Source() got %v, wanted nil", ast.Source())
    }
}

func TestIssuesNil(t *testing.T) {
    var iss *Issues
    iss = iss.Append(iss)
    if iss.Err() != nil {
        t.Errorf("iss.Err() got %v, wanted nil given nil issue set", iss.Err())
    }
    if len(iss.Errors()) != 0 {
        t.Errorf("iss.Errors() got %v, wanted empty value", iss.Errors())
    }
    if iss.String() != "" {
        t.Errorf("iss.String() returned %v, wanted empty value", iss.String())
    }
}

func TestIssuesEmpty(t *testing.T) {
    iss := NewIssues(common.NewErrors(nil))
    if iss.Err() != nil {
        t.Errorf("iss.Err() got %v, wanted nil given nil issue set", iss.Err())
    }
    if len(iss.Errors()) != 0 {
        t.Errorf("iss.Errors() got %v, wanted empty value", iss.Errors())
    }
    if iss.String() != "" {
        t.Errorf("iss.String() returned %v, wanted empty value", iss.String())
    }
    var iss2 *Issues
    iss3 := iss.Append(iss2)
    iss4 := iss3.Append(nil)
    if !reflect.DeepEqual(iss4, iss) {
        t.Error("Append() with a nil value resulted in the creation of a new issue set")
    }
}

func TestIssues(t *testing.T) {
    e, err := NewEnv()
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    _, iss := e.Compile("-")
    _, iss2 := e.Compile("b")
    iss = iss.Append(iss2)
    if len(iss.Errors()) != 3 {
        t.Errorf("iss.Errors() got %v, wanted 3 errors", iss.Errors())
    }

    wantIss := `ERROR: <input>:1:1: undeclared reference to 'b' (in container '')
 | -
 | ^
ERROR: <input>:1:2: Syntax error: no viable alternative at input '-'
 | -
 | .^
ERROR: <input>:1:2: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER}
 | -
 | .^`
    if iss.String() != wantIss {
        t.Errorf("iss.String() returned %v, wanted %v", iss.String(), wantIss)
    }
}

func TestFormatCELTypeEquivalence(t *testing.T) {
    values := []*Type{
        AnyType,
        MapType(StringType, DynType),
        types.NewTypeTypeWithParam(ListType(IntType)),
        TypeType,
        NullableType(DoubleType),
    }
    for _, v := range values {
        v := v
        t.Run(v.String(), func(t *testing.T) {
            celStr := FormatCELType(v)
            et, err := TypeToExprType(v)
            if err != nil {
                t.Fatalf("TypeToExprType(%v) failed: %v", v, err)
            }
            exprStr := FormatType(et)
            if celStr != exprStr {
                t.Errorf("FormatCELType(%v) got %s, wanted %s", v, celStr, exprStr)
            }
        })
    }
}

func TestEnvCheckExtendRace(t *testing.T) {
    t.Parallel()
    for i := 0; i < 500; i++ {
        wg := &sync.WaitGroup{}
        wg.Add(2)
        env, err := NewCustomEnv(StdLib())
        if err != nil {
            t.Fatalf("NewCustomEnv() failed: %v", err)
        }
        t.Run(fmt.Sprintf("Compile[%d]", i), func(t *testing.T) {
            go func() {
                defer wg.Done()
                _, _ = env.Compile(`1 + 1 * 20 < 400`)
            }()
        })
        t.Run(fmt.Sprintf("Extend[%d]", i), func(t *testing.T) {
            go func() {
                defer wg.Done()
                _, _ = env.Extend(Variable("bar", BoolType))
            }()
        })
        wg.Wait()
    }
}

func TestEnvPartialVarsError(t *testing.T) {
    env := testEnv(t)
    _, err := env.PartialVars(10)
    if err == nil {
        t.Error("env.PartialVars(10) succeeded, wanted error")
    }
}

func TestTypeProviderInterop(t *testing.T) {
    reg, err := types.NewRegistry(&proto3pb.TestAllTypes{})
    if err != nil {
        t.Fatalf("types.NewRegistry() failed: %v", err)
    }
    tests := []struct {
        name     string
        provider any
    }{
        {
            name:     "custom provider",
            provider: &customCELProvider{provider: reg},
        },
        {
            name:     "custom legacy provider",
            provider: &customLegacyProvider{provider: reg},
        },
        {
            name:     "provider",
            provider: reg,
        },
    }
    for _, tst := range tests {
        tc := tst
        t.Run(tc.name, func(t *testing.T) {
            env, err := NewEnv(CustomTypeProvider(tc.provider))
            if err != nil {
                t.Fatalf("NewEnv(CustomTypeProvider()) failed: %v", err)
            }
            // Found type
            pbType, found := env.TypeProvider().FindType("google.expr.proto3.test.TestAllTypes")
            if !found {
                t.Fatal("FindType(google.expr.proto3.test.TestAllTypes) failed")
            }
            celType, found := env.CELTypeProvider().FindStructType("google.expr.proto3.test.TestAllTypes")
            if !found {
                t.Fatal("FindStructType(google.expr.proto3.test.TestAllTypes) failed")
            }
            pbConvType, err := types.ExprTypeToType(pbType)
            if err != nil {
                t.Fatalf("types.ExprTypeToType(%v) failed: %v", pbType, err)
            }
            if !celType.IsExactType(pbConvType) {
                t.Errorf("got converted type %v, wanted %v", pbConvType, celType)
            }
            // Found field
            pbFieldType, found := env.TypeProvider().FindFieldType("google.expr.proto3.test.TestAllTypes", "single_int32")
            if !found {
                t.Fatal("FindFieldType(google.expr.proto3.test.TestAllTypes, single_int32) not found")
            }
            celFieldType, found := env.CELTypeProvider().FindStructFieldType("google.expr.proto3.test.TestAllTypes", "single_int32")
            if !found {
                t.Fatal("FindStructFieldType(google.expr.proto3.test.TestAllTypes, single_int32) not found")
            }
            pbConvFieldType, err := types.ExprTypeToType(pbFieldType.Type)
            if err != nil {
                t.Fatalf("types.ExprTypeToType(%v) failed: %v", pbFieldType.Type, err)
            }
            if !celFieldType.Type.IsExactType(pbConvFieldType) {
                t.Errorf("got converted field type %v, wanted %v", pbConvFieldType, celFieldType)
            }
            // Not found type
            _, found = env.TypeProvider().FindType("test.BadTypeName")
            if found {
                t.Fatal("FindType(test.BadTypeName) succeeded")
            }
            _, found = env.CELTypeProvider().FindStructType("test.BadTypeName")
            if found {
                t.Fatal("FindStructType(test.BadTypeName) succeeded")
            }
            // Not found field
            _, found = env.TypeProvider().FindFieldType("google.expr.proto3.test.TestAllTypes", "undefined_field")
            if found {
                t.Fatal("FindFieldType(google.expr.proto3.test.TestAllTypes, undefined_field) was found")
            }
            _, found = env.CELTypeProvider().FindStructFieldType("google.expr.proto3.test.TestAllTypes", "undefined_field")
            if found {
                t.Fatal("FindStructFieldType(google.expr.proto3.test.TestAllTypes, undefined_field) not found")
            }
        })
    }
}

func TestLibraries(t *testing.T) {
    e, err := NewEnv(OptionalTypes())
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    for _, expected := range []string{"cel.lib.std", "cel.lib.optional"} {
        if !e.HasLibrary(expected) {
            t.Errorf("Expected HasLibrary() to return true for '%s'", expected)
        }
        libMap := map[string]struct{}{}
        libraries := e.Libraries()
        for _, lib := range libraries {
            libMap[lib] = struct{}{}
        }
        if len(libraries) != 2 {
            t.Errorf("Expected HasLibrary() to contain exactly 2 libraries but got: %v", libraries)
        }

        if _, ok := libMap[expected]; !ok {
            t.Errorf("Expected Libraries() to include '%s'", expected)
        }
    }
}

func BenchmarkNewCustomEnvLazy(b *testing.B) {
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := NewCustomEnv(StdLib(), EagerlyValidateDeclarations(false))
        if err != nil {
            b.Fatalf("NewCustomEnv() failed: %v", err)
        }
    }
}

func BenchmarkNewCustomEnvEager(b *testing.B) {
    for i := 0; i < b.N; i++ {
        _, err := NewCustomEnv(StdLib(), EagerlyValidateDeclarations(true))
        if err != nil {
            b.Fatalf("NewCustomEnv() failed: %v", err)
        }
    }
}

func BenchmarkNewEnvLazy(b *testing.B) {
    for i := 0; i < b.N; i++ {
        _, err := NewEnv()
        if err != nil {
            b.Fatalf("NewEnv() failed: %v", err)
        }
    }
}

func BenchmarkNewEnvEager(b *testing.B) {
    for i := 0; i < b.N; i++ {
        env, err := NewEnv()
        if err != nil {
            b.Fatalf("NewEnv() failed: %v", err)
        }
        _, iss := env.Compile("123")
        if iss.Err() != nil {
            b.Fatalf("env.Compile(123) failed: %v", iss.Err())
        }
    }
}

func BenchmarkEnvExtendEager(b *testing.B) {
    env, err := NewEnv()
    if err != nil {
        b.Fatalf("NewEnv() failed: %v", err)
    }
    for i := 0; i < b.N; i++ {
        ext, err := env.Extend()
        if err != nil {
            b.Fatalf("env.Extend() failed: %v", err)
        }
        _, iss := ext.Compile("123")
        if iss.Err() != nil {
            b.Fatalf("env.Compile(123) failed: %v", iss.Err())
        }
    }
}

func BenchmarkEnvExtendEagerTypes(b *testing.B) {
    env, err := NewEnv(EagerlyValidateDeclarations(true))
    if err != nil {
        b.Fatalf("NewEnv() failed: %v", err)
    }
    for i := 0; i < b.N; i++ {
        ext, err := env.Extend(CustomTypeProvider(types.NewEmptyRegistry()))
        if err != nil {
            b.Fatalf("env.Extend() failed: %v", err)
        }
        _, iss := ext.Compile("123")
        if iss.Err() != nil {
            b.Fatalf("env.Compile(123) failed: %v", iss.Err())
        }
    }
}

func BenchmarkEnvExtendEagerDecls(b *testing.B) {
    env, err := NewEnv(EagerlyValidateDeclarations(true))
    if err != nil {
        b.Fatalf("NewEnv() failed: %v", err)
    }
    for i := 0; i < b.N; i++ {
        ext, err := env.Extend(
            Variable("test_var", StringType),
            Function(
                operators.In,
                Overload("string_in_string", []*Type{StringType, StringType}, BoolType),
            ),
        )
        if err != nil {
            b.Fatalf("env.Extend() failed: %v", err)
        }
        _, iss := ext.Compile("123")
        if iss.Err() != nil {
            b.Fatalf("env.Compile(123) failed: %v", iss.Err())
        }
    }
}

type customLegacyProvider struct {
    provider ref.TypeProvider
}

func (p *customLegacyProvider) EnumValue(enumName string) ref.Val {
    return p.provider.EnumValue(enumName)
}

func (p *customLegacyProvider) FindIdent(identName string) (ref.Val, bool) {
    return p.provider.FindIdent(identName)
}

func (p *customLegacyProvider) FindType(typeName string) (*exprpb.Type, bool) {
    return p.provider.FindType(typeName)
}

func (p *customLegacyProvider) FindFieldType(structType, fieldName string) (*ref.FieldType, bool) {
    return p.provider.FindFieldType(structType, fieldName)
}

func (p *customLegacyProvider) NewValue(structType string, fields map[string]ref.Val) ref.Val {
    return p.provider.NewValue(structType, fields)
}

type customCELProvider struct {
    provider types.Provider
}

func (p *customCELProvider) EnumValue(enumName string) ref.Val {
    return p.provider.EnumValue(enumName)
}

func (p *customCELProvider) FindIdent(identName string) (ref.Val, bool) {
    return p.provider.FindIdent(identName)
}

func (p *customCELProvider) FindStructType(typeName string) (*types.Type, bool) {
    return p.provider.FindStructType(typeName)
}

func (p *customCELProvider) FindStructFieldNames(typeName string) ([]string, bool) {
    return p.provider.FindStructFieldNames(typeName)
}

func (p *customCELProvider) FindStructFieldType(structType, fieldName string) (*types.FieldType, bool) {
    return p.provider.FindStructFieldType(structType, fieldName)
}

func (p *customCELProvider) NewValue(structType string, fields map[string]ref.Val) ref.Val {
    return p.provider.NewValue(structType, fields)
}
