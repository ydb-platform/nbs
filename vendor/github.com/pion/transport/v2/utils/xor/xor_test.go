// SPDX-FileCopyrightText: 2013 The Go Authors. All rights reserved.
// SPDX-License-Identifier: BSD-3-Clause

package xor

import (
    "bytes"
    "crypto/rand"
    "fmt"
    "io"
    "testing"
)

func TestXOR(t *testing.T) {
    for j := 1; j <= 1024; j++ {
        if testing.Short() && j > 16 {
            break
        }
        for alignP := 0; alignP < 2; alignP++ {
            for alignQ := 0; alignQ < 2; alignQ++ {
                for alignD := 0; alignD < 2; alignD++ {
                    p := make([]byte, j)[alignP:]
                    q := make([]byte, j)[alignQ:]
                    d0 := make([]byte, j+alignD+1)
                    d0[j+alignD] = 42
                    d1 := d0[alignD : j+alignD]
                    d2 := make([]byte, j+alignD)[alignD:]
                    if _, err := io.ReadFull(rand.Reader, p); err != nil {
                        t.Fatal(err)
                    }
                    if _, err := io.ReadFull(rand.Reader, q); err != nil {
                        t.Fatal(err)
                    }
                    XorBytes(d1, p, q)
                    n := min(p, q)
                    for i := 0; i < n; i++ {
                        d2[i] = p[i] ^ q[i]
                    }
                    if !bytes.Equal(d1, d2) {
                        t.Errorf(
                            "p: %#v, q: %#v, "+
                                "expect: %#v, "+
                                "result: %#v",
                            p, q, d2, d1,
                        )
                    }
                    if d0[j+alignD] != 42 {
                        t.Error("guard overwritten")
                    }
                }
            }
        }
    }
}

func min(a, b []byte) int {
    n := len(a)
    if len(b) < n {
        n = len(b)
    }
    return n
}

func BenchmarkXORAligned(b *testing.B) {
    dst := make([]byte, 1<<15)
    data0 := make([]byte, 1<<15)
    data1 := make([]byte, 1<<15)
    sizes := []int64{1 << 3, 1 << 7, 1 << 11, 1 << 15}
    for _, size := range sizes {
        b.Run(fmt.Sprintf("%dBytes", size), func(b *testing.B) {
            s0 := data0[:size]
            s1 := data1[:size]
            b.SetBytes(size)
            for i := 0; i < b.N; i++ {
                XorBytes(dst, s0, s1)
            }
        })
    }
}

func BenchmarkXORUnalignedDst(b *testing.B) {
    dst := make([]byte, 1<<15+1)
    data0 := make([]byte, 1<<15)
    data1 := make([]byte, 1<<15)
    sizes := []int64{1 << 3, 1 << 7, 1 << 11, 1 << 15}
    for _, size := range sizes {
        b.Run(fmt.Sprintf("%dBytes", size), func(b *testing.B) {
            s0 := data0[:size]
            s1 := data1[:size]
            b.SetBytes(size)
            for i := 0; i < b.N; i++ {
                XorBytes(dst[1:], s0, s1)
            }
        })
    }
}
