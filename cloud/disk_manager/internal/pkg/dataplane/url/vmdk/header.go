package vmdk

////////////////////////////////////////////////////////////////////////////////

const sparseMagicNumber = 0x564d444b // little endian for "VMDK"

const (
	newLine     = '\n'
	space       = ' '
	caretReturn = '\r'
)

const (
	flagUseRedundantGrainDirectory = 1 << 1
	flagUseZeroedGrainTableEntries = 1 << 2
	flagDataCompressed             = 1 << 16
	flagDataHasMarkers             = 1 << 17
)

const (
	// Compression algorithm is always Deflate for stream optimized images
	// and always None for monolithic sparse images according to docs.
	compressionDeflate = 1
)

const sectorSize = 512

const gdReadFooter = 0xffffffffffffffff

////////////////////////////////////////////////////////////////////////////////

type header struct {
	MagicNumber        uint32
	Version            uint32
	Flags              uint32
	Capacity           sectors
	GrainSize          sectors
	DescriptorOffset   sectors
	DescriptorSize     sectors
	NumGTEsPerGT       uint32
	RgdOffset          sectors
	GdOffset           sectors
	OverHead           sectors
	UncleanShutdown    uint8
	SingleEndLineChar  uint8
	NonEndLineChar     uint8
	DoubleEndLineChar1 uint8
	DoubleEndLineChar2 uint8
	CompressAlgorithm  uint16
}

func (h header) validate() bool {
	if h.SingleEndLineChar != newLine {
		return false
	}

	if h.NonEndLineChar != space {
		return false
	}

	if h.DoubleEndLineChar1 != caretReturn {
		return false
	}

	if h.DoubleEndLineChar2 != newLine {
		return false
	}

	return true
}

type sectors uint64

func (s sectors) bytes() uint64 {
	return uint64(s) * sectorSize
}
