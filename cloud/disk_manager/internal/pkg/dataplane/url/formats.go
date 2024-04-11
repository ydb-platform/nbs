package url

import (
	"bytes"
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

//////////////////////////////////////////////////////////////////////////////

type formatMagic struct {
	format ImageFormat
	magic  []byte
	offset int
}

var formatMagics = []formatMagic{
	{
		format: ImageFormatQCOW2,
		magic:  []byte{0x51, 0x46, 0x49, 0xFB}, // QFI\xfb
	},
	{
		format: ImageFormatVMDK,
		magic:  []byte{0x4b, 0x44, 0x4d, 0x56}, // KDMV
	},
	{
		format: ImageFormatVHDX,
		magic:  []byte{0x76, 0x68, 0x64, 0x78, 0x66, 0x69, 0x6c, 0x65}, // vhdxfile
	},
	{
		format: ImageFormatVHD,
		magic:  []byte{0x63, 0x6f, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x78}, // connectix
	},
	{
		format: ImageFormatVDI,
		magic:  []byte{0x7f, 0x10, 0xda, 0xbe},
		offset: 0x40,
	},
}

const magicBufferSize = 68 // Should be enough for all formats.

type ImageFormat string

const (
	ImageFormatRaw   ImageFormat = "raw"
	ImageFormatQCOW2 ImageFormat = "qcow2"
	ImageFormatVMDK  ImageFormat = "vmdk"
	ImageFormatVHDX  ImageFormat = "vhdx"
	ImageFormatVHD   ImageFormat = "vhd" // Also known as vpc.
	ImageFormatVDI   ImageFormat = "vdi"
)

//////////////////////////////////////////////////////////////////////////////

func (f ImageFormat) IsSupported() bool {
	switch f {
	case ImageFormatRaw, ImageFormatQCOW2, ImageFormatVMDK, ImageFormatVHD:
		return true
	}

	return false
}

//////////////////////////////////////////////////////////////////////////////

func guessImageFormat(
	ctx context.Context,
	reader common.Reader,
) (ImageFormat, error) {

	buffer := make([]byte, magicBufferSize)
	_, err := reader.Read(ctx, 0, buffer)
	if err != nil {
		return "", err
	}

	for _, magic := range formatMagics {
		if hasAtOffset(buffer, magic.magic, magic.offset) {
			return magic.format, nil
		}
	}

	return ImageFormatRaw, nil
}

func hasAtOffset(buffer []byte, magic []byte, offset int) bool {
	if len(buffer) < offset+len(magic) {
		return false
	}

	return bytes.HasPrefix(buffer[offset:], magic)
}
