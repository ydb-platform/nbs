package qcow2

import (
	"bytes"
	"encoding/binary"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

////////////////////////////////////////////////////////////////////////////////

// MAGIC "QFI\xfb".
var qcow2Magic = []byte{0x51, 0x46, 0x49, 0xFB}

func checkMagic(magic uint32) bool {
	dst := [4]byte{}
	binary.BigEndian.PutUint32(dst[:], magic)
	return !bytes.Equal(dst[:], qcow2Magic)
}

var (
	maxClusterSize = uint64(2 << 20)
	maxL1Size      = uint64(32 << 20)
	l1OffsetMask   = uint64(0x00fffffffffffe00)
	l2OffsetMask   = uint64(0x00fffffffffffe00)

	qcow2CompressedSectorSize = uint64(512)
	qcow2FlagCompressed       = uint64(1 << 62)
	qcow2FlagZero             = uint64(1 << 0)
)

////////////////////////////////////////////////////////////////////////////////

type qcow2ClusterType uint64

const (
	qcow2ClusterTypeUnallocated qcow2ClusterType = iota
	qcow2ClusterTypeZeroPlain
	qcow2ClusterTypeZeroAllocated
	qcow2ClusterTypeNormal
	qcow2ClusterTypeCompressed
)

func computeClusterType(l2Entry uint64) qcow2ClusterType {
	if l2Entry&qcow2FlagCompressed != 0 {
		return qcow2ClusterTypeCompressed
	}

	if l2Entry&qcow2FlagZero != 0 {
		if l2Entry&l2OffsetMask != 0 {
			return qcow2ClusterTypeZeroAllocated
		}

		return qcow2ClusterTypeZeroPlain
	}

	if !(l2Entry&l2OffsetMask != 0) {
		return qcow2ClusterTypeUnallocated
	}

	return qcow2ClusterTypeNormal
}

////////////////////////////////////////////////////////////////////////////////

// Represents header of qcow2 image format.
type header struct {
	Magic                 uint32 //     [0:3] QCOW magic string ("QFI\xfb").
	Version               uint32 //     [4:7] Version number.
	BackingFileOffset     uint64 //    [8:15] Offset into the image file at which the backing file name is stored.
	BackingFileSize       uint32 //   [16:19] Length of the backing file name in bytes.
	ClusterBits           uint32 //   [20:23] Number of bits that are used for addressing an offset whithin a cluster.
	Size                  uint64 //   [24:31] Virtual disk size in bytes.
	CryptMethod           uint32 //   [32:35] Encryption method.
	L1Size                uint32 //   [36:39] Number of entries in the active L1 table.
	L1TableOffset         uint64 //   [40:47] Offset into the image file at which the active L1 table starts.
	RefcountTableOffset   uint64 //   [48:55] Offset into the image file at which the refcount table starts.
	RefcountTableClusters uint32 //   [56:59] Number of clusters that the refcount table occupies.
	NbSnapshots           uint32 //   [60:63] Number of snapshots contained in the image.
	SnapshotsOffset       uint64 //   [64:71] Offset into the image file at which the snapshot table starts.
	IncompatibleFeatures  uint64 //   [72:79] for version >= 3: Bitmask of incompatible feature.
	CompatibleFeatures    uint64 //   [80:87] for version >= 3: Bitmask of compatible feature.
	AutoclearFeatures     uint64 //   [88:95] for version >= 3: Bitmask of auto-clear feature.
	RefcountOrder         uint32 //   [96:99] for version >= 3: Describes the width of a reference count block entry.
	HeaderLength          uint32 // [100:103] for version >= 3: Length of the header structure in bytes.
}

////////////////////////////////////////////////////////////////////////////////

type imageMapEntry struct {
	zero             bool
	data             bool
	rawOffset        uint64
	compressedOffset uint64
	compressedSize   uint64
}

func (e *imageMapEntry) mergeable(other imageMapEntry) bool {
	return e.zero == other.zero &&
		e.data == other.data &&
		e.rawOffset == other.rawOffset &&
		e.compressedSize == 0 &&
		other.compressedSize == 0
}

func (e *imageMapEntry) dumpToItem(item *common.ImageMapItem) {
	if e.data {
		item.Zero = false
		item.Data = true
	} else {
		item.Zero = true
		item.Data = false
	}

	if e.rawOffset != 0 {
		item.RawOffset = new(uint64)
		*item.RawOffset = e.rawOffset
	}

	if e.compressedOffset != 0 {
		item.CompressedOffset = new(uint64)
		*item.CompressedOffset = e.compressedOffset
		item.CompressedSize = new(uint64)
		*item.CompressedSize = e.compressedSize
		item.CompressionType = common.CompressionTypeFlate
	}
}
