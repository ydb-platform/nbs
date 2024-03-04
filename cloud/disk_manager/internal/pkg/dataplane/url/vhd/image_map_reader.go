package vhd

import (
	"context"
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

// Implementation is based on main source of information:
// https://learn.microsoft.com/en-us/windows/win32/vstor/about-vhd

////////////////////////////////////////////////////////////////////////////////

const (
	sectorLength     = uint32(512)
	unusedTableEntry = uint32(0xFFFFFFFF)
	batEntrySize     = uint64(4)
)

////////////////////////////////////////////////////////////////////////////////

func NewImageMapReader(reader common.Reader) *ImageMapReader {
	return &ImageMapReader{
		reader: reader,
	}
}

type ImageMapReader struct {
	reader common.Reader
	header header
	footer footer
	bat    []uint32
}

func (r *ImageMapReader) Size() uint64 {
	return r.footer.CurrentSize
}

func (r *ImageMapReader) Read(ctx context.Context) ([]common.ImageMapItem, error) {
	// Because the hard disk footer is a crucial part of the hard disk image,
	// the footer is mirrored as a header at the front of the file for purposes
	// of redundancy.
	err := r.readFooter(ctx)
	if err != nil {
		return []common.ImageMapItem{}, err
	}

	err = r.readHeader(ctx)
	if err != nil {
		return []common.ImageMapItem{}, err
	}

	err = r.readBAT(ctx)
	if err != nil {
		return []common.ImageMapItem{}, err
	}

	var items []common.ImageMapItem
	var item common.ImageMapItem
	var entry imageMapEntry

	for _, batEntry := range r.bat {
		offset := item.Start + item.Length
		imageMapEntrySize := min(uint64(r.header.BlockSize), r.Size()-offset)

		newEntry := imageMapEntry{
			hasData: false,
		}
		if batEntry != unusedTableEntry {
			newEntry = imageMapEntry{
				hasData: true,
				offset:  r.getBlockDataAddress(batEntry),
			}
		}

		if entry.mergeable(newEntry) {
			item.Length += imageMapEntrySize
		} else {
			if item.Length != 0 {
				entry.dumpToItem(&item)
				items = append(items, item)
			}

			item = common.ImageMapItem{
				Start:  offset,
				Length: imageMapEntrySize,
			}

			entry = newEntry
		}
	}

	entry.dumpToItem(&item)
	items = append(items, item)
	return items, nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *ImageMapReader) readFooter(ctx context.Context) error {
	footerSize := uint64(unsafe.Sizeof(r.footer))
	if r.reader.Size() < footerSize {
		return common.NewSourceInvalidError(
			"image size %v is less than footer size %v",
			r.reader.Size(),
			footerSize,
		)
	}

	err := r.reader.ReadBinary(
		ctx,
		0, // start
		footerSize,
		binary.BigEndian,
		&r.footer,
	)
	if err != nil {
		return err
	}

	return r.footer.validate()
}

func (r *ImageMapReader) readHeader(ctx context.Context) error {
	headerSize := uint64(unsafe.Sizeof(r.header))
	if r.reader.Size() < headerOffset+headerSize {
		return common.NewSourceInvalidError(
			"image size %v is less than header offset and header size %v",
			r.reader.Size(),
			headerOffset+headerSize,
		)
	}

	err := r.reader.ReadBinary(
		ctx,
		headerOffset,
		headerSize,
		binary.BigEndian,
		&r.header,
	)
	if err != nil {
		return err
	}

	return r.header.validate()
}

func (r *ImageMapReader) readBAT(ctx context.Context) error {
	r.bat = make([]uint32, r.header.MaxTableEntries)

	return r.reader.ReadBinary(
		ctx,
		r.header.TableOffset,
		r.getBATSizeInBytes(),
		binary.BigEndian,
		&r.bat,
	)
}

func (r *ImageMapReader) getBATSizeInBytes() uint64 {
	return uint64(r.header.MaxTableEntries) * batEntrySize
}

// GetBitmapSizeInBytes returns the size of the 'block bitmap section' that
// stores the state of the sectors in block's 'data section'. This means the
// number of bits in the bitmap is equivalent to the number of sectors in
// 'data section', dividing this number by 8 will yield the number of bytes
// required to store the bitmap. As per vhd specification sectors per block must
// be power of two. The sector length is always 512 bytes.
// This means the block size will be power of two as well e.g. 512 * 2^3,
// 512 * 2^4, 512 * 2^5 etc..
func (r *ImageMapReader) getBitmapSizeInBytes() uint32 {
	return r.header.BlockSize / sectorLength / 8
}

// GetSectorPaddedBitmapSizeInBytes returns the size of the 'block bitmap
// section' in bytes which is padded to a 512-byte sector boundary.
// The bitmap of a block is always padded to a 512-byte sector boundary.
func (r *ImageMapReader) getSectorPaddedBitmapSizeInBytes() uint64 {
	sectorSizeInBytes := float64(sectorLength)
	bitmapSizeInBytes := float64(r.getBitmapSizeInBytes())
	return uint64(
		math.Ceil(bitmapSizeInBytes/sectorSizeInBytes) * sectorSizeInBytes,
	)
}

// GetBitmapAddress returns the address of the 'block bitmap section' of a given
// block. Address is the absolute byte offset of the 'block bitmap section'. A
// block consists of 'block bitmap section' and 'data section'
func (r *ImageMapReader) getBitmapAddress(batEntry uint32) uint64 {
	return uint64(batEntry) * uint64(sectorLength)
}

// GetBlockDataAddress returns the address of the 'data section' of a given
// block. Address is the absolute byte offset of the 'data section'. A block
// consists of 'block bitmap section' and 'data section'
func (r *ImageMapReader) getBlockDataAddress(batEntry uint32) uint64 {
	return r.getBitmapAddress(batEntry) + r.getSectorPaddedBitmapSizeInBytes()
}
