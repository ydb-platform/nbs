// Package cityhash implements the CityHash family of non-cryptographic string
// hashing functions.
//
// The implementation is a fairly straightforward transliteration of the C++
// code from https://github.com/google/cityhash.  Most of the comments from the
// original are preserved here, except where they are obviously not true in the
// transliterated code.
package cityhash

// Some primes between 2^63 and 2^64 for various uses.
const (
	k0 = uint64(0xc3a5c85c97cb3127)
	k1 = uint64(0xb492b66fbe98f273)
	k2 = uint64(0x9ae16a3b2f90404f)
	k3 = uint64(0xc949d7c7509e6557)
)

// Magic numbers for 32-bit hashing.  Copied from Murmur3.
const (
	c1 = uint32(0xcc9e2d51)
	c2 = uint32(0x1b873593)
)

// Hash64 returns a 64-bit hash for a slice of bytes.
func Hash64(s []byte) uint64 {
	n := uint64(len(s))
	if n <= 32 {
		if n <= 16 {
			return hash64Len0to16(s)
		}
		return hash64Len17to32(s)
	} else if n <= 64 {
		return hash64Len33to64(s)
	}

	// For strings over 64 bytes we hash the end first, and then as we loop we
	// keep 56 bytes of state: v, w, x, y, and z.
	x := fetch64(s)
	y := fetch64(s[n-16:]) ^ k1
	z := fetch64(s[n-56:]) ^ k0

	v1, v2 := weakHashLen32WithSeeds(s[n-64:], n, y)
	w1, w2 := weakHashLen32WithSeeds(s[n-32:], n*k1, k0)
	z += shiftMix(v2) * k1
	x = ror64(z+x, 39) * k1
	y = ror64(y, 33) * k1

	// Decrease n to the nearest multiple of 64, and operate on 64-byte chunks.
	n = (n - 1) &^ 63
	for {
		x = ror64(x+y+v1+fetch64(s[16:]), 37) * k1
		y = ror64(y+v2+fetch64(s[48:]), 42) * k1
		x ^= w2
		y ^= v1
		z = ror64(z^w1, 33)
		v1, v2 = weakHashLen32WithSeeds(s, v2*k1, x+w1)
		w1, w2 = weakHashLen32WithSeeds(s[32:], z+w2, y)
		z, x = x, z
		s = s[64:]
		n -= 64
		if n == 0 {
			break
		}
	}
	return hash64Len16(hash64Len16(v1, w1)+shiftMix(y)*k1+z, hash64Len16(v2, w2)+x)
}

// Hash64WithSeed returns a 64-bit hash for s that includes seed.
func Hash64WithSeed(s []byte, seed uint64) uint64 {
	return Hash64WithSeeds(s, k2, seed)
}

// Hash64WithSeeds returns a 64-bit hash for s that includes the two seed
// values.
func Hash64WithSeeds(s []byte, seed0, seed1 uint64) uint64 {
	return hash64Len16(Hash64(s)-seed0, seed1)
}

// Hash32 returns a 32-bit hash for s.
func hash32(s []byte) uint32 {
	n := uint32(len(s))
	if n <= 24 {
		if n <= 12 {
			if n <= 4 {
				return hash32Len0to4(s)
			}
			return hash32Len5to12(s)
		}
		return hash32Len13to24(s)
	}

	// n > 24
	h := n
	g := c1 * n
	f := g

	a0 := ror32(fetch32(s[n-4:])*c1, 17) * c2
	a1 := ror32(fetch32(s[n-8:])*c1, 17) * c2
	a2 := ror32(fetch32(s[n-16:])*c1, 17) * c2
	a3 := ror32(fetch32(s[n-12:])*c1, 17) * c2
	a4 := ror32(fetch32(s[n-20:])*c1, 17) * c2

	const magic = 0xe6546b64
	h ^= a0
	h = ror32(h, 19)
	h = h*5 + magic
	h ^= a2
	h = ror32(h, 19)
	h = h*5 + magic
	g ^= a1
	g = ror32(g, 19)
	g = g*5 + magic
	g ^= a3
	g = ror32(g, 19)
	g = g*5 + magic
	f += a4
	f = ror32(f, 19)
	f = f*5 + magic
	for i := (n - 1) / 20; i != 0; i-- {
		a0 := ror32(fetch32(s)*c1, 17) * c2
		a1 := fetch32(s[4:])
		a2 := ror32(fetch32(s[8:])*c1, 17) * c2
		a3 := ror32(fetch32(s[12:])*c1, 17) * c2
		a4 := fetch32(s[16:])
		h ^= a0
		h = ror32(h, 18)
		h = h*5 + magic
		f += a1
		f = ror32(f, 19)
		f = f * c1
		g += a2
		g = ror32(g, 18)
		g = g*5 + magic
		h ^= a3 + a1
		h = ror32(h, 19)
		h = h*5 + magic
		g ^= a4
		g = bswap32(g) * 5
		h += a4 * 5
		h = bswap32(h)
		f += a0
		f, g, h = g, h, f // a.k.a. PERMUTE3
		s = s[20:]
	}
	g = ror32(g, 11) * c1
	g = ror32(g, 17) * c1
	f = ror32(f, 11) * c1
	f = ror32(f, 17) * c1
	h = ror32(h+g, 19)
	h = h*5 + magic
	h = ror32(h, 17) * c1
	h = ror32(h+f, 19)
	h = h*5 + magic
	h = ror32(h, 17) * c1
	return h
}

// Hash128 returns a 128-bit hash value for s.
func Hash128(s []byte) (lo, hi uint64) {
	n := uint64(len(s))
	if n >= 16 {
		return Hash128WithSeed(s[16:], fetch64(s)^k3, fetch64(s[8:]))
	} else if n >= 8 {
		return Hash128WithSeed(s[:0], fetch64(s)^(n*k0), fetch64(s[n-8:])^k1)
	}
	return Hash128WithSeed(s, k0, k1)
}

// Hash128WithSeed returns a 128-bit hash value for s that includes the given
// 128-bit seed.
func Hash128WithSeed(s []byte, seed0, seed1 uint64) (lo, hi uint64) {
	if len(s) < 128 {
		return cityMurmur(s, seed0, seed1)
	}

	// We expect len >= 128 to be the common case.
	// Keep 56 bytes of state: v, w, x, y, and z.
	n := uint64(len(s))
	x := seed0
	y := seed1
	z := n * k1
	v1 := ror64(y^k1, 49)*k1 + fetch64(s)
	v2 := ror64(v1, 42)*k1 + fetch64(s[8:])
	w1 := ror64(y+z, 35)*k1 + x
	w2 := ror64(x+fetch64(s[88:]), 53) * k1

	// This is the same inner loop as Hash64, manually unrolled.
	t := s
	for n >= 128 {
		// Iteration 1
		x = ror64(x+y+v1+fetch64(t[16:]), 37) * k1
		y = ror64(y+v2+fetch64(t[48:]), 42) * k1
		x ^= w2
		y ^= v1
		z = ror64(z^w1, 33)
		v1, v2 = weakHashLen32WithSeeds(t, v2*k1, x+w1)
		w1, w2 = weakHashLen32WithSeeds(t[32:], z+w2, y)
		x, z = z, x
		t = t[64:]

		// Iteration 2
		x = ror64(x+y+v1+fetch64(t[16:]), 37) * k1
		y = ror64(y+v2+fetch64(t[48:]), 42) * k1
		x ^= w2
		y ^= v1
		z = ror64(z^w1, 33)
		v1, v2 = weakHashLen32WithSeeds(t, v2*k1, x+w1)
		w1, w2 = weakHashLen32WithSeeds(t[32:], z+w2, y)
		x, z = z, x
		t = t[64:]

		n -= 128
	}
	y += ror64(w1, 37)*k0 + z
	x += ror64(v1+z, 49) * k0

	// Here, unlike in Hash64, we didn't do the tail block ahead of time.
	// We hash in 32-byte blocks working back-to-front, including as many bytes
	// as necessary from the chunk prior to t to ensure we have a whole number
	// of blocks.
	tail := s[len(s)-128:]
	for pos := 0; pos < int(n); pos += 32 {
		offset := len(tail) - pos - 32
		block := tail[offset:]

		y = ror64(y-x, 42)*k0 + v2
		w1 += fetch64(block[16:])
		x = ror64(x, 49)*k0 + w1
		w1 += v1
		v1, v2 = weakHashLen32WithSeeds(block, v1, v2)
	}

	// At this point our 48 bytes of state should contain more than
	// enough information for a strong 128-bit hash.  We use two
	// different 48-byte-to-8-byte hashes to get a 16-byte final result.
	x = hash64Len16(x, v1)
	y = hash64Len16(y, w1)
	return hash64Len16(x+v2, w2) + y, hash64Len16(x+w2, y+v2)
}

// Hash128To64 returns a 64-bit hash value for an input of 128 bits.
func Hash128To64(lo, hi uint64) uint64 {
	// Murmur-inspired hashing.
	const multiplier = 0x9ddfea08eb382d69

	a := (lo ^ hi) * multiplier
	a ^= (a >> 47)
	b := (hi ^ a) * multiplier
	b ^= (b >> 47)
	b *= multiplier
	return b
}
