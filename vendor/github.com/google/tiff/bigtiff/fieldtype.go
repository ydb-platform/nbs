// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bigtiff

import (
    "encoding/binary"
    "fmt"
    "reflect"

    "github.com/google/tiff"
)

/* Field type definitions
16 = LONG8    64-bit unsigned integer.
17 = SLONG8    64-bit signed integer.
18 = IFD8    64-bit unsigned integer offset value
*/

/* tiff.FieldTypeRepr */
func reprLong8(in []byte, bo binary.ByteOrder) string  { return fmt.Sprintf("%d", bo.Uint64(in)) }
func reprSLong8(in []byte, bo binary.ByteOrder) string { return fmt.Sprintf("%d", int64(bo.Uint64(in))) }

/* tiff.FieldTypeValuer */
func rvalLong8(in []byte, bo binary.ByteOrder) reflect.Value { return reflect.ValueOf(bo.Uint64(in)) }
func rvalSLong8(in []byte, bo binary.ByteOrder) reflect.Value {
    return reflect.ValueOf(int64(bo.Uint64(in)))
}

/* reflect.Type */
var (
    typU64 = reflect.TypeOf(uint64(0)) // LONG8, IFD8
    typI64 = reflect.TypeOf(int64(0))  // SLONG8
)

var (
    FTLong8  = tiff.NewFieldType(16, "LONG8", 8, false, reprLong8, rvalLong8, typU64)
    FTSLong8 = tiff.NewFieldType(17, "SLONG8", 8, true, reprSLong8, rvalSLong8, typI64)
    FTIFD8   = tiff.NewFieldType(18, "IFD8", 8, false, reprLong8, rvalLong8, typU64)
)

var BTFieldTypeSet = tiff.NewFieldTypeSet("BigTIFF")

func init() {
    BTFieldTypeSet.Register(FTLong8)
    BTFieldTypeSet.Register(FTSLong8)
    BTFieldTypeSet.Register(FTIFD8)

    BTFieldTypeSet.Lock()

    tiff.DefaultFieldTypeSpace.RegisterFieldTypeSet(BTFieldTypeSet)
}
