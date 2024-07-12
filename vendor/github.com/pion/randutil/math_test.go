package randutil

import (
    "regexp"
    "testing"
)

func TestMathRandomGenerator(t *testing.T) {
    g := NewMathRandomGenerator()
    isLetter := regexp.MustCompile(`^[a-zA-Z]+$`).MatchString

    for i := 0; i < 10000; i++ {
        s := g.GenerateString(10, runesAlpha)
        if len(s) != 10 {
            t.Error("Generator returned invalid length")
        }
        if !isLetter(s) {
            t.Errorf("Generator returned unexpected character: %s", s)
        }
    }
}

func TestIntn(t *testing.T) {
    g := NewMathRandomGenerator()

    min := 100
    max := 0
    for i := 0; i < 10000; i++ {
        r := g.Intn(100)
        if r < 0 || r >= 100 {
            t.Fatalf("Out of range of Intn(100): %d", r)
        }
        if r < min {
            min = r
        }
        if r > max {
            max = r
        }
    }
    if min > 10 {
        t.Error("Value around lower boundary was not generated")
    }
    if max < 90 {
        t.Error("Value around upper boundary was not generated")
    }
}

func TestUint64(t *testing.T) {
    g := NewMathRandomGenerator()

    min := uint64(0xFFFFFFFFFFFFFFFF)
    max := uint64(0)
    for i := 0; i < 10000; i++ {
        r := g.Uint64()
        if r < min {
            min = r
        }
        if r > max {
            max = r
        }
    }
    if min > 0x1000000000000000 {
        t.Error("Value around lower boundary was not generated")
    }
    if max < 0xF000000000000000 {
        t.Error("Value around upper boundary was not generated")
    }
}

func TestUint32(t *testing.T) {
    g := NewMathRandomGenerator()

    min := uint32(0xFFFFFFFF)
    max := uint32(0)
    for i := 0; i < 10000; i++ {
        r := g.Uint32()
        if r < min {
            min = r
        }
        if r > max {
            max = r
        }
    }
    if min > 0x10000000 {
        t.Error("Value around lower boundary was not generated")
    }
    if max < 0xF0000000 {
        t.Error("Value around upper boundary was not generated")
    }
}
