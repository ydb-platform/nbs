package cityhash

import "testing"

var data = make([]byte, dataSize)

func init() {
	a := uint64(9)
	b := uint64(777)

	for i := 0; i < len(data); i++ {
		a = (a^(a>>41))*k0 + b
		b = (b^(b>>41))*k0 + uint64(i)
		data[i] = byte(b >> 37)
	}
}

func TestCityHash(t *testing.T) {
	// These tests are converted from city-test.cc.
	// data is a pseudo-random vector of bytes generated in init (C++: setup).
	// testData contains the vectors of expected results (C++: testdata).
	//
	// Each position of a testData vector contains one of the expected results;
	// the tests hard-code the specific offsets.
	const (
		ch64        = 0
		ch64Seed    = 1
		ch64Seeds   = 2
		ch128Lo     = 3
		ch128Hi     = 4
		ch128SeedLo = 5
		ch128SeedHi = 6
	)
	for i := 0; i < len(testData)-1; i++ {
		pos := i * i
		end := pos + i
		bits := data[pos:end]

		if got, want := Hash64(bits), testData[i][ch64]; got != want {
			t.Errorf("[%d] Hash64 %+v: got %x, want %x", i, bits, got, want)
		}

		if got, want := Hash64WithSeed(bits, seed0), testData[i][ch64Seed]; got != want {
			t.Errorf("[%d] Hash64WithSeed %+v %x: got %x, want %x", i, bits, seed0, got, want)
		}

		if got, want := Hash64WithSeeds(bits, seed0, seed1), testData[i][ch64Seeds]; got != want {
			t.Errorf("[%d] Hash64WithSeeds %+v %x|%x: got %x, want %x",
				i, bits, seed0, seed1, got, want)
		}

		{
			lo, hi := Hash128(bits)
			wantLo := testData[i][ch128Lo]
			wantHi := testData[i][ch128Hi]
			if lo != wantLo || hi != wantHi {
				t.Errorf("[%d] Hash128 %+v lo: got %x|%x, want %x|%x",
					i, bits, lo, hi, wantLo, wantHi)
			}
		}
		{
			lo, hi := Hash128WithSeed(bits, seed0, seed1)
			wantLo := testData[i][ch128SeedLo]
			wantHi := testData[i][ch128SeedHi]
			if lo != wantLo || hi != wantHi {
				t.Errorf("[%d] Hash128WithSeed %+v %x|%x lo: got %x|%x, want %x|%x",
					i, bits, seed0, seed1, lo, hi, wantLo, wantHi)
			}

		}
	}
}
