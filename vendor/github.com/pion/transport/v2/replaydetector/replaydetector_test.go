// SPDX-FileCopyrightText: 2023 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package replaydetector

import (
    "reflect"
    "testing"
)

func TestReplayDetector(t *testing.T) {
    const largeSeq = 0x100000000000
    cases := map[string]struct {
        windowSize   uint
        maxSeq       uint64
        input        []uint64
        valid        []bool
        expected     []uint64
        expectedWrap []uint64 // nil means it's same as expected
    }{
        "Continuous": {
            16, 0x0000FFFFFFFFFFFF,
            []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true, true,
                true,
            },
            []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
            nil,
        },
        "ValidLargeJump": {
            16, 0x0000FFFFFFFFFFFF,
            []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, largeSeq, 11, largeSeq + 1, largeSeq + 2, largeSeq + 3},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true,
            },
            []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, largeSeq, largeSeq + 1, largeSeq + 2, largeSeq + 3},
            nil,
        },
        "InvalidLargeJump": {
            16, 0x0000FFFFFFFFFFFF,
            []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, largeSeq, 11, 12, 13, 14, 15},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                false, true, true, true, true, true,
            },
            []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15},
            nil,
        },
        "DuplicateAfterValidJump": {
            196, 0x0000FFFFFFFFFFFF,
            []uint64{0, 1, 2, 129, 0, 1, 2},
            []bool{
                true, true, true, true, true, true, true,
            },
            []uint64{0, 1, 2, 129},
            nil,
        },
        "DuplicateAfterInvalidJump": {
            196, 0x0000FFFFFFFFFFFF,
            []uint64{0, 1, 2, 128, 0, 1, 2},
            []bool{
                true, true, true, false, true, true, true,
            },
            []uint64{0, 1, 2},
            nil,
        },
        "ContinuousOffset": {
            16, 0x0000FFFFFFFFFFFF,
            []uint64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true,
            },
            []uint64{100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114},
            nil,
        },
        "Reordered": {
            128, 0x0000FFFFFFFFFFFF,
            []uint64{96, 64, 16, 80, 32, 48, 8, 24, 88, 40, 128, 56, 72, 112, 104, 120},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true,
            },
            []uint64{96, 64, 16, 80, 32, 48, 8, 24, 88, 40, 128, 56, 72, 112, 104, 120},
            nil,
        },
        "Old": {
            100, 0x0000FFFFFFFFFFFF,
            []uint64{24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 128, 8, 16},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true,
            },
            []uint64{24, 32, 40, 48, 56, 64, 72, 80, 88, 96, 104, 112, 120, 128},
            nil,
        },
        "ContinuousReplayed": {
            8, 0x0000FFFFFFFFFFFF,
            []uint64{16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true, true, true, true, true,
            },
            []uint64{16, 17, 18, 19, 20, 21, 22, 23, 24, 25},
            nil,
        },
        "ReplayedLater": {
            128, 0x0000FFFFFFFFFFFF,
            []uint64{16, 32, 48, 64, 80, 96, 112, 128, 16, 32, 48, 64, 80, 96, 112, 128},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true,
            },
            []uint64{16, 32, 48, 64, 80, 96, 112, 128},
            nil,
        },
        "ReplayedQuick": {
            128, 0x0000FFFFFFFFFFFF,
            []uint64{16, 16, 32, 32, 48, 48, 64, 64, 80, 80, 96, 96, 112, 112, 128, 128},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
                true, true, true, true, true, true,
            },
            []uint64{16, 32, 48, 64, 80, 96, 112, 128},
            nil,
        },
        "Strict": {
            0, 0x0000FFFFFFFFFFFF,
            []uint64{1, 3, 2, 4, 5, 6, 7, 8, 9, 10},
            []bool{
                true, true, true, true, true, true, true, true, true, true,
            },
            []uint64{1, 3, 4, 5, 6, 7, 8, 9, 10},
            nil,
        },
        "Overflow": {
            128, 0x0000FFFFFFFFFFFF,
            []uint64{0x0000FFFFFFFFFFFE, 0x0000FFFFFFFFFFFF, 0x0001000000000000, 0x0001000000000001},
            []bool{
                true, true, true, true,
            },
            []uint64{0x0000FFFFFFFFFFFE, 0x0000FFFFFFFFFFFF},
            nil,
        },
        "WrapContinuous": {
            64, 0xFFFF,
            []uint64{0xFFFC, 0xFFFD, 0xFFFE, 0xFFFF, 0x0000, 0x0001, 0x0002, 0x0003},
            []bool{
                true, true, true, true, true, true, true, true,
            },
            []uint64{0xFFFC, 0xFFFD, 0xFFFE, 0xFFFF},
            []uint64{0xFFFC, 0xFFFD, 0xFFFE, 0xFFFF, 0x0000, 0x0001, 0x0002, 0x0003},
        },
        "WrapReordered": {
            64, 0xFFFF,
            []uint64{0xFFFD, 0xFFFC, 0x0002, 0xFFFE, 0x0000, 0x0001, 0xFFFF, 0x0003},
            []bool{
                true, true, true, true, true, true, true, true,
            },
            []uint64{0xFFFD, 0xFFFC, 0xFFFE, 0xFFFF},
            []uint64{0xFFFD, 0xFFFC, 0x0002, 0xFFFE, 0x0000, 0x0001, 0xFFFF, 0x0003},
        },
        "WrapReorderedReplayed": {
            64, 0xFFFF,
            []uint64{0xFFFD, 0xFFFC, 0xFFFC, 0x0002, 0xFFFE, 0xFFFC, 0x0000, 0x0001, 0x0001, 0xFFFF, 0x0001, 0x0003},
            []bool{
                true, true, true, true, true, true, true, true, true, true, true, true,
            },
            []uint64{0xFFFD, 0xFFFC, 0xFFFE, 0xFFFF},
            []uint64{0xFFFD, 0xFFFC, 0x0002, 0xFFFE, 0x0000, 0x0001, 0xFFFF, 0x0003},
        },
    }
    for name, c := range cases {
        c := c
        if c.expectedWrap == nil {
            c.expectedWrap = c.expected
        }
        t.Run(name, func(t *testing.T) {
            for typeName, typ := range map[string]struct {
                newFunc  func(uint, uint64) ReplayDetector
                expected []uint64
            }{
                "NoWrap": {
                    newFunc:  New,
                    expected: c.expected,
                },
                "Wrap": {
                    newFunc:  WithWrap,
                    expected: c.expectedWrap,
                },
            } {
                typ := typ
                t.Run(typeName, func(t *testing.T) {
                    det := typ.newFunc(c.windowSize, c.maxSeq)
                    var out []uint64
                    for i, seq := range c.input {
                        accept, ok := det.Check(seq)
                        if ok {
                            if c.valid[i] {
                                out = append(out, seq)
                                accept()
                            }
                        }
                    }
                    if !reflect.DeepEqual(typ.expected, out) {
                        t.Errorf("Wrong replay detection result:\nexpected: %v\ngot:      %v",
                            typ.expected, out,
                        )
                    }
                })
            }
        })
    }
}
