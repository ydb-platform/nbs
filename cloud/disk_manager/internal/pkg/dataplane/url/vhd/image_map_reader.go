package vhd

import (
	"context"
	"encoding/binary"
	"unsafe"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

// Implementation is based on main source of information:
// https://learn.microsoft.com/en-us/windows/win32/vstor/about-vhd

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

	return []common.ImageMapItem{}, nil // TODO: Implement.
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
		binary.LittleEndian,
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
		binary.LittleEndian,
		&r.header,
	)
	if err != nil {
		return err
	}

	return r.header.validate()
}
