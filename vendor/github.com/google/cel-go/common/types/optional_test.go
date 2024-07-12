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

package types

import (
    "fmt"
    "reflect"
    "testing"

    "github.com/google/cel-go/common/types/ref"
)

func TestOptionalOptionalOf(t *testing.T) {
    opt := OptionalOf(IntOne)
    if !opt.HasValue() {
        t.Error("OptionalOf(1) returned result with a value")
    }
}

func TestOptionalGetValue(t *testing.T) {
    opt := OptionalOf(IntOne)
    if opt.GetValue() != IntOne {
        t.Errorf("opt.GetValue() got %v, wanted 1", opt.GetValue())
    }
    if !IsError(OptionalNone.GetValue()) {
        t.Errorf("OptionalNone.GetValue() got %v, wanted error", OptionalNone.GetValue())
    }
}

func TestOptionalConvertToNative(t *testing.T) {
    out, err := OptionalNone.ConvertToNative(reflect.TypeOf(1))
    if err == nil {
        t.Errorf("OptionalNone.GetValue() got %v, wanted error", out)
    }
    out, err = OptionalOf(String("hello")).ConvertToNative(reflect.TypeOf(""))
    if err != nil {
        t.Fatalf("OptionalOf('hello').ConvertToNative(string) failed: %v", err)
    }
    if out != "hello" {
        t.Errorf("OptionalOf('hello').ConvertToNative(string) got %v, wanted 'hello'", out)
    }
}

func TestOptionalConvertToType(t *testing.T) {
    if OptionalNone.ConvertToType(OptionalType) != OptionalNone {
        t.Errorf("ConvertToType(OptionalType) got %v, wanted optional", OptionalNone.ConvertToType(OptionalType))
    }
    if OptionalNone.ConvertToType(TypeType) != OptionalType {
        t.Errorf("ConvertToType(TypeType) got %v, wanted a type", OptionalNone.ConvertToType(TypeType))
    }
    if !IsError(OptionalNone.ConvertToType(ErrType)) {
        t.Errorf("ConvertToType(ErrType) got %v, wanted error", OptionalNone.ConvertToType(ErrType))
    }
}

func TestOptionalEqual(t *testing.T) {
    tests := []struct {
        a   ref.Val
        b   ref.Val
        out Bool
    }{
        {
            a:   OptionalNone,
            b:   OptionalNone,
            out: True,
        },
        {
            a:   IntOne,
            b:   OptionalNone,
            out: False,
        },
        {
            a:   IntOne,
            b:   OptionalNone,
            out: False,
        },
        {
            a:   OptionalOf(IntOne),
            b:   OptionalNone,
            out: False,
        },
        {
            a:   OptionalNone,
            b:   OptionalOf(IntOne),
            out: False,
        },
        {
            a:   OptionalOf(IntOne),
            b:   OptionalOf(Double(0.0)),
            out: False,
        },
        {
            a:   OptionalOf(IntOne),
            b:   OptionalOf(Double(1.0)),
            out: True,
        },
    }

    for i, tst := range tests {
        tc := tst
        t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
            if tc.a.Equal(tc.b) != tc.out {
                t.Errorf("%v.Equal(%v) got %v, wanted %v", tc.a, tc.b, !tc.out, tc.out)
            }
        })
    }
}

func TestOptionalType(t *testing.T) {
    if OptionalOf(False).Type() != OptionalType {
        t.Errorf("OptionalOf(false).Type() got %v, wanted optional", OptionalOf(False).Type())
    }
}

func TestOptionalValue(t *testing.T) {
    if OptionalOf(False).Value() != false {
        t.Errorf("OptionalOf(false).Value() got %v, wanted false", OptionalOf(False).Value())
    }
    if OptionalNone.Value() != nil {
        t.Errorf("OptionalNone.Value() got %v, wanted nil", OptionalNone.Value())
    }
}
