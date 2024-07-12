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
    "reflect"
    "testing"

    "google.golang.org/protobuf/proto"

    anypb "google.golang.org/protobuf/types/known/anypb"
    structpb "google.golang.org/protobuf/types/known/structpb"
    wrapperspb "google.golang.org/protobuf/types/known/wrapperspb"
)

func TestBytesAdd(t *testing.T) {
    if !Bytes("hello").Add(Bytes("world")).Equal(Bytes("helloworld")).(Bool) {
        t.Error("Byte ranges were not successfully added.")
    }
    if !IsError(Bytes("hello").Add(String("world"))) {
        t.Error("Types combined without conversion.")
    }
}

func TestBytesCompare(t *testing.T) {
    if !Bytes("1234").Compare(Bytes("2345")).Equal(IntNegOne).(Bool) {
        t.Error("Comparison did not yield -1")
    }
    if !Bytes("2345").Compare(Bytes("1234")).Equal(IntOne).(Bool) {
        t.Error("Comparison did not yield 1")
    }
    if !Bytes("2345").Compare(Bytes("2345")).Equal(IntZero).(Bool) {
        t.Error("Comparison did not yield 0")
    }
    if !IsError(Bytes("1").Compare(String("1"))) {
        t.Error("Comparison permitted without type conversion")
    }
}

func TestBytesConvertToNative_Any(t *testing.T) {
    val, err := Bytes("123").ConvertToNative(anyValueType)
    if err != nil {
        t.Error(err)
    }
    want, err := anypb.New(wrapperspb.Bytes([]byte("123")))
    if err != nil {
        t.Error(err)
    }
    if !proto.Equal(val.(proto.Message), want) {
        t.Errorf("Got %v, wanted %v", val, want)
    }
}

func TestBytesConvertToNative_ByteSlice(t *testing.T) {
    val, err := Bytes("123").ConvertToNative(reflect.TypeOf([]byte{}))
    if err != nil || !bytes.Equal(val.([]byte), []byte{49, 50, 51}) {
        t.Error("Got unexpected value, wanted []byte{49, 50, 51}", err, val)
    }
}

func TestBytesConvertToNative_Error(t *testing.T) {
    val, err := Bytes("123").ConvertToNative(reflect.TypeOf(""))
    if err == nil {
        t.Errorf("Got '%v', expected error", val)
    }
}

func TestBytesConvertToNative_Json(t *testing.T) {
    val, err := Bytes("123").ConvertToNative(jsonValueType)
    if err != nil {
        t.Error(err)
    }
    want := structpb.NewStringValue("MTIz")
    if !proto.Equal(val.(proto.Message), want) {
        t.Errorf("Got %v, wanted %v", val, want)
    }
}

func TestBytesConvertToNative_Wrapper(t *testing.T) {
    val, err := Bytes("123").ConvertToNative(byteWrapperType)
    if err != nil {
        t.Error(err)
    }
    want := wrapperspb.Bytes([]byte("123"))
    if !proto.Equal(val.(proto.Message), want) {
        t.Errorf("Got %v, wanted %v", val, want)
    }
}

func TestBytesConvertToType(t *testing.T) {
    if !Bytes("hello world").ConvertToType(BytesType).Equal(Bytes("hello world")).(Bool) {
        t.Error("Unsupported type conversion to bytes")
    }
    if !Bytes("hello world").ConvertToType(StringType).Equal(String("hello world")).(Bool) {
        t.Error("Unsupported type conversion to string")
    }
    if !Bytes("hello world").ConvertToType(TypeType).Equal(BytesType).(Bool) {
        t.Error("Unsupported type conversion to type")
    }
    if !IsError(Bytes("hello").ConvertToType(IntType)) {
        t.Errorf("Got value, expected error")
    }
}

func TestBytesIsZeroValue(t *testing.T) {
    if Bytes("non-zero").IsZeroValue() {
        t.Error("Bytes('non-zero').IsZeroValue() returned true, wanted false.")
    }
    if !Bytes("").IsZeroValue() {
        t.Error("Bytes('').IsZeroValue() returned false, wanted true")
    }
}

func TestBytesSize(t *testing.T) {
    if !Bytes("1234567890").Size().Equal(Int(10)).(Bool) {
        t.Error("Unexpected byte count.")
    }
}
