// Copyright 2018 Google LLC
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

package types

import (
    "bytes"
    "fmt"
    "reflect"
    "sort"
    "strings"
    "testing"
    "time"

    "github.com/google/cel-go/common/types/ref"
    "github.com/google/cel-go/common/types/traits"
    "google.golang.org/protobuf/proto"

    proto3pb "github.com/google/cel-go/test/proto3pb"
    exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
    anypb "google.golang.org/protobuf/types/known/anypb"
    dpb "google.golang.org/protobuf/types/known/durationpb"
    structpb "google.golang.org/protobuf/types/known/structpb"
    tpb "google.golang.org/protobuf/types/known/timestamppb"
    wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRegistryCopy(t *testing.T) {
    reg := NewEmptyRegistry()
    reg2 := reg.Copy()
    if !reflect.DeepEqual(reg, reg2) {
        t.Fatal("type registry copy did not produce equivalent values.")
    }
    reg = newTestRegistry(t)
    reg2 = reg.Copy()
    if !reflect.DeepEqual(reg, reg2) {
        t.Fatal("type registry copy did not produce equivalent values.")
    }
}

func TestRegistryRegisterType(t *testing.T) {
    reg := newTestRegistry(t)
    err := reg.RegisterType(
        NewTypeValue("http.Request", traits.ReceiverType),
        NewObjectType("http.Request", traits.ReceiverType),
    )
    if err == nil {
        t.Error("RegisterType() for differing type definitions with the same name did not fail")
    }
}

func TestRegistryRegisterTypeNoConflict(t *testing.T) {
    reg := newTestRegistry(t)
    err := reg.RegisterType(
        NewOpaqueType("http.Request", NewTypeParamType("T")),
        NewOpaqueType("http.Request", NewTypeParamType("V")),
    )
    if err != nil {
        t.Errorf("RegisterType() failed for equivalent types: %v", err)
    }
}

func TestRegistryRegisterTypeConflict(t *testing.T) {
    reg := newTestRegistry(t)
    err := reg.RegisterType(
        NewOpaqueType("http.Request", NewTypeParamType("T"), NewTypeParamType("V")),
        NewOpaqueType("http.Request", NewTypeParamType("V")),
    )
    if err == nil {
        t.Error("RegisterType() for differing type definitions with the same name did not fail")
    }
}

func TestRegistryEnumValue(t *testing.T) {
    reg := newTestRegistry(t)
    err := reg.RegisterDescriptor(proto3pb.GlobalEnum_GOO.Descriptor().ParentFile())
    if err != nil {
        t.Fatalf("RegisterDescriptor() failed: %v", err)
    }
    enumVal := reg.EnumValue("google.expr.proto3.test.GlobalEnum.GOO")
    if Int(proto3pb.GlobalEnum_GOO.Number()) != enumVal.(Int) {
        t.Errorf("enum values were not equal between registry and proto: %v", enumVal)
    }
    enumVal2, found := reg.FindIdent("google.expr.proto3.test.GlobalEnum.GOO")
    if !found {
        t.Fatal("Ident not found google.expr.proto3.test.GlobalEnum.GOO")
    }
    if enumVal.(Int) != enumVal2.(Int) {
        t.Errorf("got enum value %v, wanted %v", enumVal2, enumVal)
    }
}

func TestRegistryFindStructType(t *testing.T) {
    reg := newTestRegistry(t)
    err := reg.RegisterDescriptor(proto3pb.GlobalEnum_GOO.Descriptor().ParentFile())
    if err != nil {
        t.Fatalf("RegisterDescriptor() failed: %v", err)
    }
    msgTypeName := ".google.expr.proto3.test.TestAllTypes"
    exprType, found := reg.FindType(msgTypeName)
    if !found {
        t.Fatalf("FindType() did not find: %q", msgTypeName)
    }
    celType, found := reg.FindStructType(msgTypeName)
    if !found {
        t.Fatalf("FindStructType() did not find %q", msgTypeName)
    }
    exprConvType, err := ExprTypeToType(exprType)
    if err != nil {
        t.Fatalf("ExprTypeToType(%v) failed: %v", exprType, err)
    }
    if !exprConvType.IsExactType(celType) {
        t.Errorf("Got %v type, wanted %v", exprConvType, celType)
    }
    _, found = reg.FindType(msgTypeName + "Undefined")
    if found {
        t.Fatalf("FindType() found: %q", msgTypeName+"Undefined")
    }
    _, found = reg.FindStructType(msgTypeName + "Undefined")
    if found {
        t.Fatalf("FindStructType() found: %q", msgTypeName+"Undefined")
    }
}

func TestRegistryFindStructFieldNames(t *testing.T) {
    reg := newTestRegistry(t, &exprpb.Decl{}, &exprpb.Reference{})
    tests := []struct {
        typeName string
        fields   []string
    }{
        {
            typeName: "google.api.expr.v1alpha1.Reference",
            fields:   []string{"name", "overload_id", "value"},
        },
        {
            typeName: "google.api.expr.v1alpha1.Decl",
            fields:   []string{"name", "ident", "function"},
        },
        {
            typeName: "invalid.TypeName",
            fields:   []string{},
        },
    }

    for _, tst := range tests {
        tc := tst
        t.Run(fmt.Sprintf("%s", tc.typeName), func(t *testing.T) {
            fields, _ := reg.FindStructFieldNames(tc.typeName)
            sort.Strings(fields)
            sort.Strings(tc.fields)
            if !reflect.DeepEqual(fields, tc.fields) {
                t.Errorf("got %v, wanted %v", fields, tc.fields)
            }
        })
    }
}

func TestRegistryFindStructFieldType(t *testing.T) {
    reg := newTestRegistry(t)
    err := reg.RegisterDescriptor(proto3pb.GlobalEnum_GOO.Descriptor().ParentFile())
    if err != nil {
        t.Fatalf("RegisterDescriptor() failed: %v", err)
    }
    msgTypeName := ".google.expr.proto3.test.TestAllTypes"
    tests := []struct {
        typeName string
        field    string
        found    bool
    }{
        {
            typeName: msgTypeName,
            field:    "single_bool",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "single_nested_message",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "single_nested_message",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "standalone_enum",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "single_duration",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "single_timestamp",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "single_any",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "single_int64_wrapper",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "repeated_bool",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "map_string_string",
            found:    true,
        },
        {
            typeName: msgTypeName,
            field:    "double_bool",
            found:    false,
        },
        {
            typeName: msgTypeName + "Undefined",
            field:    "map_string_string",
            found:    false,
        },
    }

    for _, tst := range tests {
        tc := tst
        t.Run(fmt.Sprintf("%s.%s", tc.typeName, tc.field), func(t *testing.T) {
            // When the field is expected to be found, test parity of the results
            if tc.found {
                refField, found := reg.FindFieldType(tc.typeName, tc.field)
                if !found {
                    t.Fatalf("FindFieldType() did not find: %s.%s", tc.typeName, tc.field)
                }
                celField, found := reg.FindStructFieldType(tc.typeName, tc.field)
                if !found {
                    t.Fatalf("FindStructFieldType() found: %s.%s", tc.typeName, tc.field)
                }
                convCelFieldType, err := ExprTypeToType(refField.Type)
                if err != nil {
                    t.Fatalf("ExprTypeToType(%v) failed: %v", refField.Type, err)
                }
                if !convCelFieldType.IsExactType(celField.Type) {
                    t.Errorf("Got %v type, wanted %v", convCelFieldType, celField.Type)
                }
                return
            }
            // When the field is not expected to be round ensure both return not found.
            if !tc.found {
                _, found := reg.FindFieldType(tc.typeName, tc.field)
                if found {
                    t.Errorf("FindFieldType() found: %s.%s", tc.typeName, tc.field)
                }
                _, found = reg.FindStructFieldType(tc.typeName, tc.field)
                if found {
                    t.Errorf("FindStructFieldType() found: %s.%s", tc.typeName, tc.field)
                }
            }
        })
    }
}

func TestRegistryNewValue(t *testing.T) {
    reg := newTestRegistry(t, &proto3pb.TestAllTypes{}, &exprpb.SourceInfo{})
    tests := []struct {
        typeName string
        fields   map[string]ref.Val
        out      proto.Message
    }{
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields:   map[string]ref.Val{},
            out:      &proto3pb.TestAllTypes{},
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "standalone_enum": Int(1),
            },
            out: &proto3pb.TestAllTypes{
                StandaloneEnum: proto3pb.TestAllTypes_BAR,
            },
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "single_int32_wrapper": Int(123),
                "single_int64_wrapper": NullValue,
            },
            out: &proto3pb.TestAllTypes{
                SingleInt32Wrapper: wrapperspb.Int32(123),
            },
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "repeated_int64": reg.NativeToValue([]int64{3, 2, 1}),
            },
            out: &proto3pb.TestAllTypes{
                RepeatedInt64: []int64{3, 2, 1},
            },
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "single_nested_enum": Int(2),
            },
            out: &proto3pb.TestAllTypes{
                NestedType: &proto3pb.TestAllTypes_SingleNestedEnum{
                    SingleNestedEnum: proto3pb.TestAllTypes_BAZ,
                },
            },
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "single_value": True,
            },
            out: &proto3pb.TestAllTypes{
                SingleValue: structpb.NewBoolValue(true),
            },
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "single_value": reg.NativeToValue([]any{"hello", 10.2}),
            },
            out: &proto3pb.TestAllTypes{
                SingleValue: structpb.NewListValue(
                    &structpb.ListValue{
                        Values: []*structpb.Value{
                            structpb.NewStringValue("hello"),
                            structpb.NewNumberValue(10.2),
                        },
                    },
                ),
            },
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "repeated_nested_message": reg.NativeToValue([]any{
                    &proto3pb.TestAllTypes_NestedMessage{Bb: 123},
                }),
            },
            out: &proto3pb.TestAllTypes{
                RepeatedNestedMessage: []*proto3pb.TestAllTypes_NestedMessage{{Bb: 123}},
            },
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "map_int64_nested_type": reg.NativeToValue(map[int64]any{
                    1234: &proto3pb.NestedTestAllTypes{Payload: &proto3pb.TestAllTypes{SingleInt32: 1234}},
                }),
            },
            out: &proto3pb.TestAllTypes{
                MapInt64NestedType: map[int64]*proto3pb.NestedTestAllTypes{
                    1234: {Payload: &proto3pb.TestAllTypes{SingleInt32: 1234}},
                },
            },
        },
        {
            typeName: "google.api.expr.v1alpha1.SourceInfo",
            fields: map[string]ref.Val{
                "location":     String("TestRegistryNewValue"),
                "line_offsets": reg.NativeToValue([]int64{0, 2}),
                "positions":    reg.NativeToValue(map[int64]int64{1: 2, 2: 4}),
            },
            out: &exprpb.SourceInfo{
                Location:    "TestRegistryNewValue",
                LineOffsets: []int32{0, 2},
                Positions:   map[int64]int32{1: 2, 2: 4},
            },
        },
    }
    for i, tst := range tests {
        tc := tst
        t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
            out := reg.NewValue(tc.typeName, tc.fields)
            if IsError(out) {
                t.Fatalf("reg.NewValue(%s, %v) failed: %v", tc.typeName, tc.fields, out)
            }
            if !proto.Equal(tc.out, out.Value().(proto.Message)) {
                t.Errorf("reg.NewValue() got %v, wanted %v", out, tc.out)
            }
        })
    }
}

func TestRegistryNewValueErrors(t *testing.T) {
    reg := newTestRegistry(t, &proto3pb.TestAllTypes{}, &exprpb.SourceInfo{})
    tests := []struct {
        typeName string
        fields   map[string]ref.Val
        err      string
    }{
        {
            typeName: "google.expr.proto3.test.TestAllType",
            fields:   map[string]ref.Val{},
            err:      "unknown type",
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "undefined": Int(1),
            },
            err: "no such field",
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "single_int32_wrapper": True,
            },
            err: "type conversion error",
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "repeated_int64": reg.NativeToValue([]float64{1.0, 2.3}),
            },
            err: "type conversion error",
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "repeated_int64": Int(10),
            },
            err: "unsupported field type",
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "map_string_string": NullValue,
            },
            err: "unsupported field type",
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "map_string_string": reg.NativeToValue(map[string]int{"hello": 1}),
            },
            err: "type conversion error",
        },
        {
            typeName: "google.expr.proto3.test.TestAllTypes",
            fields: map[string]ref.Val{
                "map_string_string": reg.NativeToValue(map[int]int{1: 1}),
            },
            err: "type conversion error",
        },
    }
    for i, tst := range tests {
        tc := tst
        t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
            out := reg.NewValue(tc.typeName, tc.fields)
            if !IsError(out) {
                t.Fatalf("reg.NewValue(%s, %v) got %v, wanted error", tc.typeName, tc.fields, out)
            }
            err := out.(*Err)
            if !strings.Contains(err.Error(), tc.err) {
                t.Errorf("reg.NewValue() got error %v, wanted error %s", err, tc.err)
            }
        })
    }
}

func TestRegistryGetters(t *testing.T) {
    reg := newTestRegistry(t, &exprpb.ParsedExpr{})
    if sourceInfo := reg.NewValue(
        "google.api.expr.v1alpha1.SourceInfo",
        map[string]ref.Val{
            "location":     String("TestTypeRegistryGetFieldValue"),
            "line_offsets": NewDynamicList(reg, []int64{0, 2}),
            "positions":    NewDynamicMap(reg, map[int64]int64{1: 2, 2: 4}),
        }); IsError(sourceInfo) {
        t.Error(sourceInfo)
    } else {
        si := sourceInfo.(traits.Indexer)
        if loc := si.Get(String("location")); IsError(loc) {
            t.Error(loc)
        } else if loc.(String) != "TestTypeRegistryGetFieldValue" {
            t.Errorf("Expected %s, got %s", "TestTypeRegistryGetFieldValue", loc)
        }
        if pos := si.Get(String("positions")); IsError(pos) {
            t.Error(pos)
        } else if pos.Equal(NewDynamicMap(reg, map[int64]int32{1: 2, 2: 4})) != True {
            t.Errorf("Expected map[int64]int32, got %v", pos)
        } else if posKeyVal := pos.(traits.Indexer).Get(Int(1)); IsError(posKeyVal) {
            t.Error(posKeyVal)
        } else if posKeyVal.(Int) != 2 {
            t.Error("Expected value to be int64, not int32")
        }
        if offsets := si.Get(String("line_offsets")); IsError(offsets) {
            t.Error(offsets)
        } else if offset1 := offsets.(traits.Lister).Get(Int(1)); IsError(offset1) {
            t.Error(offset1)
        } else if offset1.(Int) != 2 {
            t.Errorf("Expected index 1 to be value 2, was %v", offset1)
        }
    }
}

func TestConvertToNative(t *testing.T) {
    reg := newTestRegistry(t, &exprpb.ParsedExpr{})

    // Core type conversion tests.
    expectValueToNative(t, True, true)
    expectValueToNative(t, True, True)
    expectValueToNative(t, NewDynamicList(reg, []Bool{True, False}), []any{true, false})
    expectValueToNative(t, NewDynamicList(reg, []Bool{True, False}), []ref.Val{True, False})
    expectValueToNative(t, Int(-1), int32(-1))
    expectValueToNative(t, Int(2), int64(2))
    expectValueToNative(t, Int(-1), Int(-1))
    expectValueToNative(t, NewDynamicList(reg, []Int{4}), []any{int64(4)})
    expectValueToNative(t, NewDynamicList(reg, []Int{5}), []ref.Val{Int(5)})
    expectValueToNative(t, Uint(3), uint32(3))
    expectValueToNative(t, Uint(4), uint64(4))
    expectValueToNative(t, Uint(5), Uint(5))
    expectValueToNative(t, NewDynamicList(reg, []Uint{4}), []any{uint64(4)})
    expectValueToNative(t, NewDynamicList(reg, []Uint{5}), []ref.Val{Uint(5)})
    expectValueToNative(t, Double(5.5), float32(5.5))
    expectValueToNative(t, Double(-5.5), float64(-5.5))
    expectValueToNative(t, NewDynamicList(reg, []Double{-5.5}), []any{-5.5})
    expectValueToNative(t, NewDynamicList(reg, []Double{-5.5}), []ref.Val{Double(-5.5)})
    expectValueToNative(t, Double(-5.5), Double(-5.5))
    expectValueToNative(t, String("hello"), "hello")
    expectValueToNative(t, String("hello"), String("hello"))
    expectValueToNative(t, NullValue, structpb.NullValue_NULL_VALUE)
    expectValueToNative(t, NullValue, NullValue)
    expectValueToNative(t, NewDynamicList(reg, []Null{NullValue}), []any{structpb.NullValue_NULL_VALUE})
    expectValueToNative(t, NewDynamicList(reg, []Null{NullValue}), []ref.Val{NullValue})
    expectValueToNative(t, Bytes("world"), []byte("world"))
    expectValueToNative(t, Bytes("world"), Bytes("world"))
    expectValueToNative(t, NewDynamicList(reg, []Bytes{Bytes("hello")}), []any{[]byte("hello")})
    expectValueToNative(t, NewDynamicList(reg, []Bytes{Bytes("hello")}), []ref.Val{Bytes("hello")})
    expectValueToNative(t, NewDynamicList(reg, []int64{1, 2, 3}), []int32{1, 2, 3})
    expectValueToNative(t, Duration{Duration: time.Duration(500)}, time.Duration(500))
    expectValueToNative(t, Duration{Duration: time.Duration(500)}, Duration{Duration: time.Duration(500)})
    expectValueToNative(t, Timestamp{Time: time.Unix(12345, 0)}, time.Unix(12345, 0))
    expectValueToNative(t, Timestamp{Time: time.Unix(12345, 0)}, Timestamp{Time: time.Unix(12345, 0)})
    expectValueToNative(t, NewDynamicMap(reg,
        map[int64]int64{1: 1, 2: 1, 3: 1}),
        map[int32]int32{1: 1, 2: 1, 3: 1})

    // Null conversion tests.
    expectValueToNative(t, Null(structpb.NullValue_NULL_VALUE), structpb.NullValue_NULL_VALUE)

    // Proto conversion tests.
    parsedExpr := &exprpb.ParsedExpr{}
    expectValueToNative(t, reg.NativeToValue(parsedExpr), parsedExpr)

    // Custom scalars
    expectValueToNative(t, Int(1), testInt(1))
    expectValueToNative(t, Int(1), testInt8(1))
    expectValueToNative(t, Int(1), testInt16(1))
    expectValueToNative(t, Int(1), testInt32(1))
    expectValueToNative(t, Int(1), testInt64(1))
    expectValueToNative(t, Uint(1), testUint(1))
    expectValueToNative(t, Uint(1), testUint8(1))
    expectValueToNative(t, Uint(1), testUint16(1))
    expectValueToNative(t, Uint(1), testUint32(1))
    expectValueToNative(t, Uint(1), testUint64(1))
    expectValueToNative(t, Double(4.5), testFloat32(4.5))
    expectValueToNative(t, Double(-5.1), testFloat64(-5.1))
    expectValueToNative(t, String("foo"), testString("foo"))
}

func TestNativeToValue_Any(t *testing.T) {
    reg := newTestRegistry(t, &exprpb.ParsedExpr{})
    // NullValue
    anyValue, err := NullValue.ConvertToNative(anyValueType)
    if err != nil {
        t.Error(err)
    }
    expectNativeToValue(t, anyValue, NullValue)

    // Json Struct
    anyValue, err = anypb.New(
        structpb.NewStructValue(
            &structpb.Struct{
                Fields: map[string]*structpb.Value{
                    "a": structpb.NewStringValue("world"),
                    "b": structpb.NewStringValue("five!"),
                },
            },
        ),
    )
    if err != nil {
        t.Error(err)
    }
    expected := NewJSONStruct(reg, &structpb.Struct{
        Fields: map[string]*structpb.Value{
            "a": structpb.NewStringValue("world"),
            "b": structpb.NewStringValue("five!"),
        },
    })
    expectNativeToValue(t, anyValue, expected)

    //Json List
    anyValue, err = anypb.New(structpb.NewListValue(
        &structpb.ListValue{
            Values: []*structpb.Value{
                structpb.NewStringValue("world"),
                structpb.NewStringValue("five!"),
            },
        },
    ))
    if err != nil {
        t.Error(err)
    }
    expectedList := NewJSONList(reg, &structpb.ListValue{
        Values: []*structpb.Value{
            structpb.NewStringValue("world"),
            structpb.NewStringValue("five!"),
        }})
    expectNativeToValue(t, anyValue, expectedList)

    // Object
    pbMessage := exprpb.ParsedExpr{
        SourceInfo: &exprpb.SourceInfo{
            LineOffsets: []int32{1, 2, 3}}}
    anyValue, err = anypb.New(&pbMessage)
    if err != nil {
        t.Error(err)
    }
    expectNativeToValue(t, anyValue, reg.NativeToValue(&pbMessage))
}

func TestNativeToValue_Json(t *testing.T) {
    reg := newTestRegistry(t, &exprpb.ParsedExpr{})
    // Json primitive conversion test.
    expectNativeToValue(t, structpb.NewBoolValue(false), False)
    expectNativeToValue(t, structpb.NewNumberValue(1.1), Double(1.1))
    expectNativeToValue(t, structpb.NewNullValue(), Null(structpb.NullValue_NULL_VALUE))
    expectNativeToValue(t, structpb.NewStringValue("hello"), String("hello"))

    // Json list conversion.
    expectNativeToValue(t,
        structpb.NewListValue(
            &structpb.ListValue{
                Values: []*structpb.Value{
                    structpb.NewStringValue("world"),
                    structpb.NewStringValue("five!"),
                },
            },
        ),
        NewJSONList(reg, &structpb.ListValue{
            Values: []*structpb.Value{
                structpb.NewStringValue("world"),
                structpb.NewStringValue("five!"),
            },
        }))

    // Json struct conversion.
    expectNativeToValue(t,
        structpb.NewStructValue(
            &structpb.Struct{
                Fields: map[string]*structpb.Value{
                    "a": structpb.NewStringValue("world"),
                    "b": structpb.NewStringValue("five!"),
                },
            },
        ),
        NewJSONStruct(reg, &structpb.Struct{
            Fields: map[string]*structpb.Value{
                "a": structpb.NewStringValue("world"),
                "b": structpb.NewStringValue("five!"),
            },
        }))

    // Proto conversion test.
    parsedExpr := &exprpb.ParsedExpr{}
    expectNativeToValue(t, parsedExpr, reg.NativeToValue(parsedExpr))
}

func TestNativeToValue_Wrappers(t *testing.T) {
    // Wrapper conversion test.
    expectNativeToValue(t, wrapperspb.Bool(true), True)
    expectNativeToValue(t, &wrapperspb.BoolValue{}, False)
    expectNativeToValue(t, &wrapperspb.BytesValue{}, Bytes{})
    expectNativeToValue(t, wrapperspb.Bytes([]byte("hi")), Bytes("hi"))
    expectNativeToValue(t, &wrapperspb.DoubleValue{}, Double(0.0))
    expectNativeToValue(t, wrapperspb.Double(6.4), Double(6.4))
    expectNativeToValue(t, &wrapperspb.FloatValue{}, Double(0.0))
    expectNativeToValue(t, wrapperspb.Float(3.0), Double(3.0))
    expectNativeToValue(t, &wrapperspb.Int32Value{}, IntZero)
    expectNativeToValue(t, wrapperspb.Int32(-32), Int(-32))
    expectNativeToValue(t, &wrapperspb.Int64Value{}, IntZero)
    expectNativeToValue(t, wrapperspb.Int64(-64), Int(-64))
    expectNativeToValue(t, &wrapperspb.StringValue{}, String(""))
    expectNativeToValue(t, wrapperspb.String("hello"), String("hello"))
    expectNativeToValue(t, &wrapperspb.UInt32Value{}, Uint(0))
    expectNativeToValue(t, wrapperspb.UInt32(32), Uint(32))
    expectNativeToValue(t, &wrapperspb.UInt64Value{}, Uint(0))
    expectNativeToValue(t, wrapperspb.UInt64(64), Uint(64))
}

func TestNativeToValue_Primitive(t *testing.T) {
    reg := newTestRegistry(t)

    // Core type conversions.
    expectNativeToValue(t, true, True)
    expectNativeToValue(t, int(-10), Int(-10))
    expectNativeToValue(t, int32(-1), Int(-1))
    expectNativeToValue(t, int64(2), Int(2))
    expectNativeToValue(t, uint(6), Uint(6))
    expectNativeToValue(t, uint32(3), Uint(3))
    expectNativeToValue(t, uint64(4), Uint(4))
    expectNativeToValue(t, float32(5.5), Double(5.5))
    expectNativeToValue(t, float64(-5.5), Double(-5.5))
    expectNativeToValue(t, "hello", String("hello"))
    expectNativeToValue(t, []byte("world"), Bytes("world"))
    expectNativeToValue(t, time.Duration(500), Duration{Duration: time.Duration(500)})
    expectNativeToValue(t, time.Unix(12345, 0), Timestamp{Time: time.Unix(12345, 0)})
    expectNativeToValue(t, dpb.New(time.Duration(500)), Duration{Duration: time.Duration(500)})
    expectNativeToValue(t, tpb.New(time.Unix(12345, 0)), Timestamp{Time: time.Unix(12345, 0)})
    expectNativeToValue(t, []int32{1, 2, 3}, NewDynamicList(reg, []int32{1, 2, 3}))
    expectNativeToValue(t, map[int32]int32{1: 1, 2: 1, 3: 1},
        NewDynamicMap(reg, map[int32]int32{1: 1, 2: 1, 3: 1}))

    // Pointers to core types.
    pBool := true
    expectNativeToValue(t, &pBool, True)
    pDub32 := float32(2.5)
    pDub64 := float64(-1000.2)
    expectNativeToValue(t, &pDub32, Double(2.5))
    expectNativeToValue(t, &pDub64, Double(-1000.2))
    pInt := int(1)
    pInt32 := int32(2)
    pInt64 := int64(-1000)
    expectNativeToValue(t, &pInt, Int(1))
    expectNativeToValue(t, &pInt32, Int(2))
    expectNativeToValue(t, &pInt64, Int(-1000))
    pStr := "hello"
    expectNativeToValue(t, &pStr, String("hello"))
    pUint := uint(1)
    pUint32 := uint32(2)
    pUint64 := uint64(1000)
    expectNativeToValue(t, &pUint, Uint(1))
    expectNativeToValue(t, &pUint32, Uint(2))
    expectNativeToValue(t, &pUint64, Uint(1000))

    // Pointers to ref.Val extensions of core types.
    rBool := True
    expectNativeToValue(t, &rBool, True)
    rDub := Double(32.1)
    expectNativeToValue(t, &rDub, rDub)
    rInt := Int(-12)
    expectNativeToValue(t, &rInt, rInt)
    rStr := String("hello")
    expectNativeToValue(t, &rStr, rStr)
    rUint := Uint(12405)
    expectNativeToValue(t, &rUint, rUint)
    rBytes := Bytes([]byte("hello"))
    expectNativeToValue(t, &rBytes, rBytes)

    // Extensions to core types.
    expectNativeToValue(t, testInt(1), Int(1))
    expectNativeToValue(t, testInt8(1), Int(1))
    expectNativeToValue(t, testInt16(1), Int(1))
    expectNativeToValue(t, testInt32(1), Int(1))
    expectNativeToValue(t, testInt64(-100), Int(-100))
    expectNativeToValue(t, testUint(1), Uint(1))
    expectNativeToValue(t, testUint8(1), Uint(1))
    expectNativeToValue(t, testUint16(1), Uint(1))
    expectNativeToValue(t, testUint32(2), Uint(2))
    expectNativeToValue(t, testUint64(3), Uint(3))
    expectNativeToValue(t, testFloat32(4.5), Double(4.5))
    expectNativeToValue(t, testFloat64(-5.1), Double(-5.1))
    expectNativeToValue(t, testString("foo"), String("foo"))

    // Null conversion test.
    expectNativeToValue(t, nil, NullValue)
    expectNativeToValue(t, structpb.NullValue_NULL_VALUE, Null(structpb.NullValue_NULL_VALUE))
}

func TestUnsupportedConversion(t *testing.T) {
    reg := newTestRegistry(t)
    if val := reg.NativeToValue(nonConvertible{}); !IsError(val) {
        t.Error("Expected error when converting non-proto struct to proto", val)
    }
}

func expectValueToNative(t *testing.T, in ref.Val, out any) {
    t.Helper()
    if val, err := in.ConvertToNative(reflect.TypeOf(out)); err != nil {
        t.Error(err)
    } else {
        var equals bool
        switch val.(type) {
        case []byte:
            equals = bytes.Equal(val.([]byte), out.([]byte))
        case proto.Message:
            equals = proto.Equal(val.(proto.Message), out.(proto.Message))
        case bool, int32, int64, uint32, uint64, float32, float64, string:
            equals = val == out
        default:
            equals = reflect.DeepEqual(val, out)
        }
        if !equals {
            t.Errorf("Unexpected conversion from expr to proto.\n"+
                "expected: %T, actual: %T", out, val)
        }
    }
}

func expectNativeToValue(t *testing.T, in any, out ref.Val) {
    t.Helper()
    reg := newTestRegistry(t, &exprpb.ParsedExpr{})
    if val := reg.NativeToValue(in); IsError(val) {
        t.Error(val)
    } else {
        if val.Equal(out) != True {
            t.Errorf("Unexpected conversion from expr to proto.\n"+
                "expected: %T, actual: %T", val, out)
        }
    }
}

func BenchmarkNativeToValue(b *testing.B) {
    reg, err := NewRegistry()
    if err != nil {
        b.Fatalf("NewRegistry() failed: %v", err)
    }
    inputs := []any{
        true,
        false,
        float32(-1.2),
        float64(-2.4),
        1,
        int32(2),
        int64(3),
        "",
        "hello",
        String("hello world"),
    }
    for _, in := range inputs {
        input := in
        b.Run(fmt.Sprintf("%T/%v", in, in), func(b *testing.B) {
            for i := 0; i < b.N; i++ {
                reg.NativeToValue(input)
            }
        })
    }
}

func BenchmarkTypeProviderNewValue(b *testing.B) {
    reg, err := NewRegistry(&exprpb.ParsedExpr{})
    if err != nil {
        b.Fatalf("NewRegistry() failed: %v", err)
    }
    for i := 0; i < b.N; i++ {
        reg.NewValue(
            "google.api.expr.v1.SourceInfo",
            map[string]ref.Val{
                "Location":    String("BenchmarkTypeProvider_NewValue"),
                "LineOffsets": NewDynamicList(reg, []int64{0, 2}),
                "Positions":   NewDynamicMap(reg, map[int64]int64{1: 2, 2: 4}),
            })
    }
}

func BenchmarkTypeProviderCopy(b *testing.B) {
    reg, err := NewRegistry()
    if err != nil {
        b.Fatalf("NewRegistry() failed: %v", err)
    }
    for i := 0; i < b.N; i++ {
        reg.Copy()
    }
}

// Helper types useful for testing extensions of primitive types.
type nonConvertible struct {
    Field string
}
type testBool bool
type testInt int
type testInt8 int8
type testInt16 int16
type testInt32 int32
type testInt64 int64
type testUint uint
type testUint8 uint8
type testUint16 uint16
type testUint32 uint32
type testUint64 uint64
type testFloat32 float32
type testFloat64 float64
type testString string

func newTestRegistry(t *testing.T, types ...proto.Message) *Registry {
    t.Helper()
    reg, err := NewRegistry(types...)
    if err != nil {
        t.Fatalf("NewRegistry(%v) failed: %v", types, err)
    }
    return reg
}
