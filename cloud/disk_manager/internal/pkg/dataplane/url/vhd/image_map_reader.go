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

func (r *ImageMapReader) ReadFooter(ctx context.Context) error {
	// Because the hard disk footer is a crucial part of the hard disk image,
	// the footer is mirrored as a header at the front of the file for purposes
	// of redundancy.
	return r.readFooter(ctx)
}

func (r *ImageMapReader) ReadHeader(ctx context.Context) error {
	// The dynamic disk header should appear on a sector (512-byte) boundary.
	return r.readHeader(ctx, 512)
}

func (r *ImageMapReader) Read(context.Context) ([]common.ImageMapItem, error) {
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

	if string(r.footer.Cookie[:]) != footerCookie {
		return common.NewSourceInvalidError(
			"Failed to check vhd footer cookie: expected - %s, actual - %s",
			footerCookie,
			r.footer.Cookie,
		)
	}

	if !r.footer.validate() {
		return common.NewSourceInvalidError(
			"Failed to check vhd footer validity: footer is corrupted",
		)
	}

	return nil
}

func (r *ImageMapReader) readHeader(ctx context.Context, offset uint64) error {
	headerSize := uint64(unsafe.Sizeof(r.header))
	if r.reader.Size() < offset+headerSize {
		return common.NewSourceInvalidError(
			"image size %v is less than header offset and header size %v",
			r.reader.Size(),
			offset+headerSize,
		)
	}

	err := r.reader.ReadBinary(
		ctx,
		offset,
		headerSize,
		binary.LittleEndian,
		&r.header,
	)
	if err != nil {
		return err
	}

	if string(r.header.Cookie[:]) != headerCookie {
		return common.NewSourceInvalidError(
			"Failed to check vhd header cookie: expected - %s, actual - %s",
			headerCookie,
			r.header.Cookie,
		)
	}

	if !r.header.validate() {
		return common.NewSourceInvalidError(
			"Failed to check vhd header validity: header is corrupted",
		)
	}

	return nil
}
