// Copyright 2019 Google LLC
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
    "testing"
    "time"

    "google.golang.org/protobuf/proto"

    "github.com/google/cel-go/checker/decls"
    "github.com/google/cel-go/common/types"

    proto3pb "github.com/google/cel-go/test/proto3pb"
    exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func TestRefValueToValueRoundTrip(t *testing.T) {
    tests := []struct {
        value any
    }{
        {value: types.NullValue},
        {value: types.Bool(true)},
        {value: types.String("abc")},
        {value: types.Double(0.0)},
        {value: types.Bytes(make([]byte, 0, 5))},
        {value: types.Int(0)},
        {value: types.Uint(0)},
        {value: types.Duration{Duration: time.Hour}},
        {value: types.Timestamp{Time: time.Unix(0, 0)}},
        {value: types.IntType},
        {value: types.NewOpaqueType("CustomType")},
        {value: map[int64]int64{1: 1}},
        {value: []any{true, "abc"}},
        {value: &proto3pb.TestAllTypes{SingleString: "abc"}},
    }

    env, err := NewEnv(Types(&proto3pb.TestAllTypes{}))
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }

    for i, tst := range tests {
        tc := tst
        t.Run(fmt.Sprintf("[%d]%v", i, tc.value), func(t *testing.T) {
            refVal := env.TypeAdapter().NativeToValue(tc.value)
            val, err := RefValueToValue(refVal)
            if err != nil {
                t.Fatalf("RefValueToValue(%v) failed with error: %v", refVal, err)
            }
            actual, err := ValueToRefValue(env.TypeAdapter(), val)
            if err != nil {
                t.Fatalf("ValueToRefValue() failed: %v", err)
            }
            if refVal.Equal(actual) != types.True {
                t.Errorf("got val %v, wanted %v", actual, refVal)
            }
        })
    }
}

func TestAstToProto(t *testing.T) {
    stdEnv, _ := NewEnv(Declarations(
        decls.NewVar("a", decls.Dyn),
        decls.NewVar("b", decls.Dyn),
    ))
    ast, iss := stdEnv.Parse("a + b")
    if iss.Err() != nil {
        t.Fatalf("Parse('a + b') failed: %v", iss.Err())
    }
    parsed, err := AstToParsedExpr(ast)
    if err != nil {
        t.Fatalf("AstToParsedExpr() failed: %v", err)
    }
    ast2 := ParsedExprToAst(parsed)
    if !proto.Equal(ast2.Expr(), ast.Expr()) {
        t.Errorf("got expr %v, wanted %v", ast2, ast)
    }
    ast3 := ParsedExprToAstWithSource(parsed, ast.Source())
    if !proto.Equal(ast3.Expr(), ast.Expr()) {
        t.Errorf("got expr %v, wanted %v", ast2, ast)
    }
    if ast3.Source() != ast.Source() {
        t.Errorf("got source %v, wanted %v", ast3.Source(), ast.Source())
    }

    _, err = AstToCheckedExpr(ast)
    if err == nil {
        t.Error("expected error converting unchecked ast")
    }
    ast, iss = stdEnv.Check(ast)
    if iss != nil && iss.Err() != nil {
        t.Fatalf("stdEnv.Check(ast) failed: %v", iss.Err())
    }
    checked, err := AstToCheckedExpr(ast)
    if err != nil {
        t.Fatalf("AstToCheckedExpr(ast) failed: %v", err)
    }
    ast4 := CheckedExprToAst(checked)
    if !proto.Equal(ast4.Expr(), ast.Expr()) {
        t.Fatalf("got ast %v, wanted %v", ast4, ast)
    }
    ast5, err := CheckedExprToAstWithSource(checked, ast.Source())
    if err != nil {
        t.Fatalf("CheckedExprToAstWithSource() failed: %v", err)
    }
    if !proto.Equal(ast5.Expr(), ast.Expr()) {
        t.Errorf("got expr %v, wanted %v", ast5, ast)
    }
    if ast5.Source() != ast.Source() {
        t.Errorf("got source %v, wanted %v", ast5.Source(), ast.Source())
    }
}

func TestAstToString(t *testing.T) {
    stdEnv, err := NewEnv()
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    in := "a + b - (c ? (-d + 4) : e)"
    ast, iss := stdEnv.Parse(in)
    if iss.Err() != nil {
        t.Fatalf("stdEnv.Parse(%q) failed: %v", in, iss.Err())
    }
    expr, err := AstToString(ast)
    if err != nil {
        t.Fatalf("AstToString(ast) failed: %v", err)
    }
    if expr != in {
        t.Errorf("got %v, wanted %v", expr, in)
    }
}

func TestCheckedExprToAstConstantExpr(t *testing.T) {
    stdEnv, err := NewEnv()
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    in := "10"
    ast, iss := stdEnv.Compile(in)
    if iss.Err() != nil {
        t.Fatalf("stdEnv.Compile(%q) failed: %v", in, iss.Err())
    }
    expr, err := AstToCheckedExpr(ast)
    if err != nil {
        t.Fatalf("AstToCheckedExpr(ast) failed: %v", err)
    }
    ast2 := CheckedExprToAst(expr)
    if !proto.Equal(ast2.Expr(), ast.Expr()) {
        t.Fatalf("got ast %v, wanted %v", ast2, ast)
    }
}

func TestCheckedExprToAstMissingInfo(t *testing.T) {
    stdEnv, err := NewEnv()
    if err != nil {
        t.Fatalf("NewEnv() failed: %v", err)
    }
    in := "10"
    ast, iss := stdEnv.Parse(in)
    if iss.Err() != nil {
        t.Fatalf("stdEnv.Compile(%q) failed: %v", in, iss.Err())
    }
    if ast.ResultType() != decls.Dyn {
        t.Fatalf("ast.ResultType() got %v, wanted 'dyn'", ast.ResultType())
    }
    expr, err := AstToParsedExpr(ast)
    if err != nil {
        t.Fatalf("AstToParsedExpr(ast) failed: %v", err)
    }
    checkedExpr := &exprpb.CheckedExpr{
        TypeMap: map[int64]*exprpb.Type{expr.GetExpr().GetId(): decls.Int},
        Expr:    expr.GetExpr(),
    }
    ast2 := CheckedExprToAst(checkedExpr)
    if !ast2.IsChecked() {
        t.Fatal("CheckedExprToAst() did not produce a 'checked' ast")
    }
    if ast2.ResultType() != decls.Int {
        t.Fatalf("ast2.ResultType() got %v, wanted 'int'", ast.ResultType())
    }
}
