package qcow2

/* this code is a very light-weight version of original qemu-img:
https://github.com/qemu/qemu/blob/master/block/qcow2.c
- does not support external files
- does not support subclusters
- does not support encryption
*/

import (
	"context"
	"encoding/binary"
	"math/bits"
	"unsafe"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type ImageMapReader struct {
	reader              common.Reader
	header              header
	clusterBits         uint64
	clusterSize         uint64
	l1Size              uint64
	l1Table             []uint64
	l2Size              uint64
	l2Bits              uint64
	l2Cache             map[uint64][]uint64
	clusterOffsetMask   uint64
	compressedSizeShift uint64
	compressedSizeMask  uint64
}

func NewImageMapReader(
	ctx context.Context,
	reader common.Reader,
) (*ImageMapReader, error) {

	imageMapReader := ImageMapReader{
		reader:  reader,
		l2Cache: make(map[uint64][]uint64),
	}

	err := imageMapReader.readHeader(ctx)
	if err != nil {
		return nil, err
	}

	return &imageMapReader, nil
}

func (r *ImageMapReader) Size() uint64 {
	return r.header.Size
}

func (r *ImageMapReader) Read(
	ctx context.Context,
) ([]common.ImageMapItem, error) {

	r.l1Table = make([]uint64, r.l1Size)
	err := r.reader.ReadBinary(
		ctx,
		r.header.L1TableOffset,
		r.l1Size*8,
		binary.BigEndian,
		&r.l1Table,
	)
	if err != nil {
		return nil, err
	}

	var items []common.ImageMapItem
	var item common.ImageMapItem
	var entry imageMapEntry

	for item.Start+item.Length < r.Size() {
		offset := item.Start + item.Length
		bytes := r.Size() - offset

		bytesRead, newEntry, err := r.readImageMapEntry(ctx, offset, bytes)
		if err != nil {
			return nil, err
		}

		if entry.mergeable(newEntry) {
			item.Length += bytesRead
		} else {
			if item.Length != 0 {
				entry.dumpToItem(&item)
				items = append(items, item)
			}

			item = common.ImageMapItem{
				Start:  offset,
				Length: bytesRead,
			}

			entry = newEntry
		}
	}

	entry.dumpToItem(&item)
	items = append(items, item)
	return items, nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *ImageMapReader) readHeader(ctx context.Context) error {
	headerSize := uint64(unsafe.Sizeof(r.header))
	if r.reader.Size() < headerSize {
		return common.NewSourceInvalidError(
			"image size %v is less than header size %v",
			r.reader.Size(),
			headerSize,
		)
	}

	err := r.reader.ReadBinary(ctx, 0, headerSize, binary.BigEndian, &r.header)
	if err != nil {
		return err
	}

	if checkMagic(r.header.Magic) {
		logging.Info(ctx,
			"invalid qcow2 magic: expected - %v, actual - %v",
			qcow2Magic,
			r.header.Magic,
		)
		return common.NewSourceInvalidError(
			"invalid qcow2 magic: expected - %v, actual - %v",
			qcow2Magic,
			r.header.Magic,
		)
	}

	if r.header.Version < 2 || r.header.Version > 3 {
		logging.Info(
			ctx,
			"unsupported qcow2 version: %d",
			r.header.Version,
		)
		return common.NewSourceInvalidError(
			"unsupported qcow2 version: %d",
			r.header.Version,
		)
	}

	if r.header.CryptMethod != 0 {
		logging.Info(
			ctx,
			"encryption is not supported, CryptMethod %v",
			r.header.CryptMethod,
		)
		return common.NewSourceInvalidError(
			"encryption is not supported, CryptMethod %v",
			r.header.CryptMethod,
		)
	}

	hasExternalFile := r.header.IncompatibleFeatures&(1<<2) != 0
	if hasExternalFile {
		return common.NewSourceInvalidError(
			"external file feature is not supported",
		)
	}

	hasCustomCompression := r.header.IncompatibleFeatures&(1<<3) != 0
	if hasCustomCompression {
		logging.Info(ctx, "custom compression feature is not supported")
		return common.NewSourceInvalidError(
			"custom compression feature is not supported",
		)
	}

	hasSubclusters := r.header.IncompatibleFeatures&(1<<4) != 0
	if hasSubclusters {
		logging.Info(ctx, "subclusters feature is not supported")
		return common.NewSourceInvalidError(
			"subclusters feature is not supported",
		)
	}

	if r.header.Size == 0 {
		return common.NewSourceInvalidError(
			"image virtual size should not be zero",
		)
	}

	r.clusterBits = uint64(r.header.ClusterBits)
	r.clusterSize = 1 << r.clusterBits
	if r.clusterSize > maxClusterSize {
		return common.NewSourceInvalidError(
			"cluster size %v exceeds the limit of %v",
			r.clusterSize,
			maxClusterSize,
		)
	}

	r.l1Size = uint64(r.header.L1Size)
	if r.l1Size > maxL1Size {
		return common.NewSourceInvalidError(
			"L1 size %v exceeds the limit of %v",
			r.l1Size,
			maxL1Size,
		)
	}

	r.l2Size = r.clusterSize / 8
	r.l2Bits = uint64(bits.TrailingZeros64(r.l2Size))

	r.clusterOffsetMask = (1 << (63 - r.clusterBits)) - 1
	r.compressedSizeShift = 62 - (r.clusterBits - 8)
	r.compressedSizeMask = (1 << (r.clusterBits - 8)) - 1

	return nil
}

func (r *ImageMapReader) countContiguousClusters(
	clusterCount uint64,
	l2Table []uint64,
	l2Index uint64,
) (uint64, error) {

	var expectedType qcow2ClusterType
	var checkOffset bool
	var expectedOffset uint64

	count := uint64(0)

	if l2Index+clusterCount > r.l2Size {
		return 0, common.NewSourceInvalidError(
			"cluster index is out of l2 table size: l2Index %v, clusterCount %v, l2Size %v",
			l2Index,
			clusterCount,
			r.l2Size,
		)
	}

	for i := uint64(0); i < clusterCount; i++ {
		l2Idx := l2Index + i
		l2Entry := l2Table[l2Idx]

		clusterType := computeClusterType(l2Entry)
		if i == 0 {
			if clusterType == qcow2ClusterTypeCompressed {
				return 1, nil
			}

			expectedType = clusterType
			expectedOffset = l2Entry & l2OffsetMask
			checkOffset = clusterType == qcow2ClusterTypeNormal ||
				clusterType == qcow2ClusterTypeZeroAllocated
		} else if clusterType != expectedType {
			break
		} else if checkOffset {
			expectedOffset += r.clusterSize
			if expectedOffset != (l2Entry & l2OffsetMask) {
				break
			}
		}

		count += 1
	}

	return count, nil
}

func (r *ImageMapReader) sizeToClusterCount(size uint64) uint64 {
	return (size + r.clusterSize - 1) >> r.clusterBits
}

func (r *ImageMapReader) offsetToL2Index(offset uint64) uint64 {
	return (offset >> r.clusterBits) & (r.l2Size - 1)
}

func (r *ImageMapReader) offsetInCluster(offset uint64) uint64 {
	return offset & (r.clusterSize - 1)
}

func (r *ImageMapReader) readL2Table(
	ctx context.Context,
	offset uint64,
) ([]uint64, error) {

	value, ok := r.l2Cache[offset]
	if ok {
		return value, nil
	}

	l2Table := make([]uint64, r.clusterSize/8)

	err := r.reader.ReadBinary(ctx, offset, r.clusterSize, binary.BigEndian, &l2Table)
	if err != nil {
		return nil, err
	}

	r.l2Cache[offset] = l2Table
	return l2Table, nil
}

func (r *ImageMapReader) readImageMapEntry(
	ctx context.Context,
	offset uint64,
	bytes uint64,
) (uint64, imageMapEntry, error) {

	var entry imageMapEntry

	l1Index := offset >> (r.l2Bits + r.clusterBits)
	l2Offset := r.l1Table[l1Index] & l1OffsetMask
	l2Index := (offset >> r.clusterBits) & (r.l2Size - 1)
	offsetInCluster := offset & (r.clusterSize - 1)

	bytesNeeded := bytes + offsetInCluster
	bytesAvailable := (r.l2Size - r.offsetToL2Index(offset)) << r.clusterBits

	if bytesNeeded > bytesAvailable {
		bytesNeeded = bytesAvailable
	}

	clusterCount := r.sizeToClusterCount(bytesNeeded)

	if l2Offset == 0 {
		return bytesAvailable - offsetInCluster, entry, nil
	}

	l2Table, err := r.readL2Table(ctx, l2Offset)
	if err != nil {
		return 0, imageMapEntry{}, err
	}

	l2Entry := l2Table[l2Index]

	clusterCount, err = r.countContiguousClusters(clusterCount, l2Table, l2Index)
	if err != nil {
		return 0, imageMapEntry{}, err
	}

	bytesAvailable = clusterCount << r.clusterBits
	if bytesAvailable > bytesNeeded {
		bytesAvailable = bytesNeeded
	}

	clusterType := computeClusterType(l2Entry)
	switch clusterType {
	case qcow2ClusterTypeCompressed:
		entry.compressedOffset = l2Entry & r.clusterOffsetMask
		compressedSectorCount := ((l2Entry >> r.compressedSizeShift) & r.compressedSizeMask) + 1
		entry.compressedSize = compressedSectorCount * qcow2CompressedSectorSize
		entry.compressedSize -= entry.compressedOffset & (qcow2CompressedSectorSize - 1)

	case qcow2ClusterTypeZeroPlain, qcow2ClusterTypeUnallocated:
		break

	case qcow2ClusterTypeZeroAllocated, qcow2ClusterTypeNormal:
		hostClusterOffset := l2Entry & l2OffsetMask
		entry.rawOffset = hostClusterOffset + offsetInCluster
		if r.offsetInCluster(hostClusterOffset) != 0 {
			return 0, imageMapEntry{}, common.NewSourceInvalidError(
				"cluster allocation offset %v is unaligned (L2 offset: %v, L2 index %v)",
				hostClusterOffset,
				l2Offset,
				l2Index,
			)
		}
	}

	if clusterType == qcow2ClusterTypeZeroPlain ||
		clusterType == qcow2ClusterTypeZeroAllocated {
		entry.zero = true
	} else if clusterType != qcow2ClusterTypeUnallocated {
		entry.data = true
	}

	return bytesAvailable - offsetInCluster, entry, nil
}
