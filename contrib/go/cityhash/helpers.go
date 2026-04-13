package cityhash

import "encoding/binary"

// A 32-bit to 32-bit integer hash copied from Murmur3.
func fmix(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// Bitwise right rotate, 64-bit.
func ror64(val, shift uint64) uint64 {
	// Avoid shifting by 64: doing so yields an undefined result.
	if shift != 0 {
		return val>>shift | val<<(64-shift)
	}
	return val
}

// Bitwise right rotate, 32-bit.
func ror32(val, shift uint32) uint32 {
	// Avoid shifting by 32: doing so yields an undefined result.
	if shift != 0 {
		return val>>shift | val<<(32-shift)
	}
	return val
}

func shiftMix(val uint64) uint64 { return val ^ (val >> 47) }

func hash64Len16(u, v uint64) uint64 { return hash128to64(u, v) }

func hash64Len16Mul(u, v, mul uint64) uint64 {
	// Murmur-inspired hashing.
	a := (u ^ v) * mul
	a ^= (a >> 47)
	b := (v ^ a) * mul
	b ^= (b >> 47)
	b *= mul
	return b
}

func hash64Len0to16(s []byte) uint64 {
	n := uint64(len(s))
	if n > 8 {
		a := fetch64(s)
		b := fetch64(s[n-8:])
		return hash64Len16(a, ror64(b+n, n)) ^ b
	}
	if n >= 4 {
		a := uint64(fetch32(s))
		return hash64Len16(n+(a<<3), uint64(fetch32(s[n-4:])))
	}
	if n > 0 {
		a := s[0]
		b := s[n>>1]
		c := s[n-1]
		y := uint32(a) + uint32(b)<<8
		z := uint32(n) + uint32(c)<<2
		return shiftMix(uint64(y)*k2^uint64(z)*k3) * k2
	}
	return k2
}

func hash64Len17to32(s []byte) uint64 {
	n := uint64(len(s))
	a := fetch64(s) * k1
	b := fetch64(s[8:])
	c := fetch64(s[n-8:]) * k2
	d := fetch64(s[n-16:]) * k0
	return hash64Len16(ror64(a-b, 43)+ror64(c, 30)+d, a+ror64(b^k3, 20)-c+n)
}

func bswap64(in uint64) uint64 {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], in)
	return binary.BigEndian.Uint64(buf[:])
}

func bswap32(in uint32) uint32 {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], in)
	return binary.BigEndian.Uint32(buf[:])
}

// Return an 8-byte hash for 33 to 64 bytes.
func hash64Len33to64(s []byte) uint64 {
	n := uint64(len(s))
	z := fetch64(s[24:])
	a := fetch64(s) + (n+fetch64(s[n-16:]))*k0
	b := ror64(a+z, 52)
	c := ror64(a, 37)
	a += fetch64(s[8:])
	c += ror64(a, 7)
	a += fetch64(s[16:])
	vf := a + z
	vs := b + ror64(a, 31) + c
	a = fetch64(s[16:]) + fetch64(s[n-32:])
	z = fetch64(s[n-8:])
	b = ror64(a+z, 52)
	c = ror64(a, 37)
	a += fetch64(s[n-24:])
	c += ror64(a, 7)
	a += fetch64(s[n-16:])
	wf := a + z
	ws := b + ror64(a, 31) + c
	r := shiftMix((vf+ws)*k2 + (wf+vs)*k0)
	return shiftMix(r*k0+vs) * k2
}

func hash128to64(lo, hi uint64) uint64 {
	// Murmur-inspired hashing.
	const mul = uint64(0x9ddfea08eb382d69)
	a := (lo ^ hi) * mul
	a ^= (a >> 47)
	b := (hi ^ a) * mul
	b ^= (b >> 47)
	b *= mul
	return b
}

var fetch64 = binary.LittleEndian.Uint64 // :: []byte -> uint64
var fetch32 = binary.LittleEndian.Uint32 // :: []byte -> uint32

// Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
// Callers do best to use "random-looking" values for a and b.
func weakHashLen32WithSeeds(s []byte, a, b uint64) (uint64, uint64) {
	// Note: Was two overloads of WeakHashLen32WithSeeds.  The second is only
	// ever called from the first, so I inlined it.
	w := fetch64(s)
	x := fetch64(s[8:])
	y := fetch64(s[16:])
	z := fetch64(s[24:])

	a += w
	b = ror64(b+a+z, 21)
	c := a
	a += x
	a += y
	b += ror64(a, 44)
	return a + z, b + c
}

func hash32Len0to4(s []byte) uint32 {
	b := uint32(0)
	c := uint32(9)

	for i := 0; i < len(s); i++ {
		v := int8(s[i])
		b = b*c1 + uint32(v)
		c ^= b
	}
	return fmix(mur(b, mur(uint32(len(s)), c)))
}

func mur(a, h uint32) uint32 {
	// Helper from Murmur3 for combining two 32-bit values.
	a *= c1
	a = ror32(a, 17)
	a *= c2
	h ^= a
	h = ror32(h, 19)
	return h*5 + 0xe6546b64
}

func hash32Len5to12(s []byte) uint32 {
	n := uint32(len(s))
	a := n
	b := n * 5
	c := uint32(9)
	d := b

	a += fetch32(s)
	b += fetch32(s[n-4:])
	c += fetch32(s[(n>>1)&4:])
	return fmix(mur(c, mur(b, mur(a, d))))
}

func hash32Len13to24(s []byte) uint32 {
	n := uint32(len(s))
	a := fetch32(s[(n>>1)-4:])
	b := fetch32(s[4:])
	c := fetch32(s[n-8:])
	d := fetch32(s[n>>1:])
	e := fetch32(s)
	f := fetch32(s[n-4:])
	h := n

	return fmix(mur(f, mur(e, mur(d, mur(c, mur(b, mur(a, h)))))))
}

// A subroutine for Hash128.  Returns a decent 128-bit hash for s based on City
// and Murmur.
func cityMurmur(s []byte, seed0, seed1 uint64) (lo, hi uint64) {
	a := seed0
	b := seed1
	var c, d uint64
	n := int64(len(s) - 16)
	if n <= 0 {
		a = shiftMix(a*k1) * k1
		c = b*k1 + hash64Len0to16(s)

		if len(s) >= 8 {
			d = shiftMix(a + fetch64(s))
		} else {
			d = shiftMix(a + c)
		}
	} else {
		c = hash64Len16(fetch64(s[len(s)-8:])+k1, a)
		d = hash64Len16(b+uint64(len(s)), c+fetch64(s[len(s)-16:]))
		a += d
		for n > 0 {
			a ^= shiftMix(fetch64(s)*k1) * k1
			a *= k1
			b ^= a
			c ^= shiftMix(fetch64(s[8:])*k1) * k1
			c *= k1
			d ^= c
			s = s[16:]
			n -= 16
		}
	}
	a = hash64Len16(a, c)
	b = hash64Len16(d, b)
	return a ^ b, hash64Len16(b, a)
}
