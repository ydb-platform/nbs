package randutil

import (
    "regexp"
    "testing"
)

func TestCryptoRandomGenerator(t *testing.T) {
    isLetter := regexp.MustCompile(`^[a-zA-Z]+$`).MatchString

    for i := 0; i < 10000; i++ {
        s, err := GenerateCryptoRandomString(10, runesAlpha)
        if err != nil {
            t.Error(err)
        }
        if len(s) != 10 {
            t.Error("Generator returned invalid length")
        }
        if !isLetter(s) {
            t.Errorf("Generator returned unexpected character: %s", s)
        }
    }
}

func TestCryptoUint64(t *testing.T) {
    min := uint64(0xFFFFFFFFFFFFFFFF)
    max := uint64(0)
    for i := 0; i < 10000; i++ {
        r, err := CryptoUint64()
        if err != nil {
            t.Fatal(err)
        }
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
