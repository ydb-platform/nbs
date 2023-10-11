package common

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

const (
	CompressionTypeFlate CompressionType = iota
	CompressionTypeZlib
)

////////////////////////////////////////////////////////////////////////////////

type CompressionType uint8

// http://manpages.ubuntu.com/manpages/trusty/man1/qemu-img.1.html (map [-f fmt] [--output=ofmt] filename)
type ImageMapItem struct {
	Start            uint64          `json:"start"`                      // Offset in logical disk represented by this image.
	Length           uint64          `json:"length"`                     // Length in bytes.
	Zero             bool            `json:"zero"`                       // Contains only zeroes.
	Data             bool            `json:"data"`                       // Contains data.
	RawOffset        *uint64         `json:"offset,omitempty"`           // Offset in image file for raw data.
	CompressedOffset *uint64         `json:"coffset,omitempty"`          // Offset in image file for compressed data.
	CompressedSize   *uint64         `json:"csize,omitempty"`            // Size of compressed data.
	CompressionType  CompressionType `json:"compression_type,omitempty"` // Compression type.
}

func (i ImageMapItem) Copy() ImageMapItem {
	res := i

	if i.RawOffset != nil {
		res.RawOffset = new(uint64)
		*res.RawOffset = *i.RawOffset
	}

	if i.CompressedOffset != nil {
		res.CompressedOffset = new(uint64)
		*res.CompressedOffset = *i.CompressedOffset
	}

	return res
}

func (i *ImageMapItem) Cut(length uint64) ImageMapItem {
	res := i.Copy()
	res.Length = length

	i.Start += length
	i.Length -= length

	if i.RawOffset != nil {
		*i.RawOffset += length
	}

	return res
}

////////////////////////////////////////////////////////////////////////////////

type ImageMapReader interface {
	Size() uint64
	Read(context.Context) ([]ImageMapItem, error)
}

////////////////////////////////////////////////////////////////////////////////

// ImageMap contains mappings from logical offset to raw offset in image file.
type ImageMap struct {
	Items []ImageMapItem
}

func (m ImageMap) Empty() bool {
	return len(m.Items) == 0
}

func (m *ImageMap) CutChunk(chunkSize uint64) ImageMap {
	var res ImageMap
	var size uint64

	for len(m.Items) != 0 && size+m.Items[0].Length <= chunkSize {
		res.Items = append(res.Items, m.Items[0])
		size += m.Items[0].Length
		m.Items = m.Items[1:]
	}

	if len(m.Items) == 0 || chunkSize <= size {
		return res
	}

	item := m.Items[0].Cut(chunkSize - size)
	res.Items = append(res.Items, item)
	return res
}
