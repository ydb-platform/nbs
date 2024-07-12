package fastuuid

import (
    "bytes"
    "crypto/rand"
    "testing"
)

func TestUUID(t *testing.T) {
    var buf [24]byte
    for i := range buf {
        buf[i] = byte(i) + 1
    }
    oldReader := rand.Reader
    rand.Reader = bytes.NewReader(buf[:])
    g, err := NewGenerator()
    rand.Reader = oldReader
    if err != nil {
        t.Fatalf("cannot make generator: %v", err)
    }
    uuid := g.Next()
    buf[0] = 1 + 1
    if uuid != buf {
        t.Fatalf("unexpected UUID; got %x; want %x", uuid, buf)
    }
    uuid = g.Next()
    buf[0] = 1 + 2
    if uuid != buf {
        t.Fatalf("unexpected next UUID; got %x; want %x", uuid, buf)
    }
}

const step = 32768

func TestUniqueness(t *testing.T) {
    g := MustNewGenerator()
    mc := make(chan map[[24]byte]int)
    const nproc = 4
    for i := 0; i < nproc; i++ {
        go func() {
            m := make(map[[24]byte]int)
            for i := 0; i < step*10; i++ {
                uuid := g.Next()
                if old, ok := m[uuid]; ok {
                    t.Errorf("non-unique uuid seq at %d, other %d", i, old)
                }
                m[uuid] = i
            }
            mc <- m
        }()
    }
    m := make(map[[24]byte]int)
    for i := 0; i < nproc; i++ {
        for uuid, iter := range <-mc {
            if old, ok := m[uuid]; ok {
                t.Errorf("non-unique uuid seq at %d, other %d", i, old)
            }
            m[uuid] = iter
        }
    }
}

func TestHex128(t *testing.T) {
    var b [24]byte
    for i := range b {
        b[i] = byte(i + 1)
    }
    // Note: byte 6 is swapped with byte 9.
    got, want := Hex128(b), "01020304-0506-4a08-8907-0b0c0d0e0f10"
    if got != want {
        t.Fatalf("unexpected Hex128 result; got %q want %q", got, want)
    }
}

var validHex128Tests = []struct {
    u     string
    valid bool
}{{
    u:     "01020304-0506-0708-090a-0b0c0d0e0f10",
    valid: true,
}, {
    u:     "01020304-0506-0708-090a-0b0c0d0e0f1",
    valid: false,
}, {
    u:     "0102030430506-0708-090a-0b0c0d0e0f1",
    valid: false,
}, {
    u:     "01020304-050630708-090a-0b0c0d0e0f1",
    valid: false,
}, {
    u:     "01020304-0506-07084090a-0b0c0d0e0f1",
    valid: false,
}, {
    u:     "01020304-0506-0708-090a50b0c0d0e0f1",
    valid: false,
}, {
    u:     "01020304-0506-0708-090a-0b0c0d0e0f1",
    valid: false,
}, {
    u:     "01020304-0506-0708-090a-0b0c0d0e0f102",
    valid: false,
}, {
    u:     "01020304-0506-0708-090a-0b0c0d0e0f1/",
    valid: false,
}}

func TestValidHex128(t *testing.T) {
    for _, test := range validHex128Tests {
        t.Run(test.u, func(t *testing.T) {
            if got := ValidHex128(test.u); got != test.valid {
                t.Fatalf("unexpected valid for %q; got %v want %v", test.u, got, test.valid)
            }
        })
    }
}

var _s string

func BenchmarkHex128(b *testing.B) {
    g := MustNewGenerator()
    for i := 0; i < b.N; i++ {
        _s = Hex128(g.Next())
    }
}

func BenchmarkNext(b *testing.B) {
    g := MustNewGenerator()
    for i := 0; i < b.N; i++ {
        g.Next()
    }
}

func BenchmarkContended(b *testing.B) {
    g := MustNewGenerator()
    b.RunParallel(func(pb *testing.PB) {
        for pb.Next() {
            g.Next()
        }
    })
}
