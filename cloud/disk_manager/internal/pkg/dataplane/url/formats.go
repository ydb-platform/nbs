package url

import (
	"bytes"
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

//////////////////////////////////////////////////////////////////////////////

const cookieBufferSize = 68 // Should be enough for all formats.

var formatCookies = map[ImageFormat][]byte{
	ImageFormatQCOW2: {0x51, 0x46, 0x49, 0xFB},                         // QFI\xfb
	ImageFormatVMDK:  {0x4b, 0x44, 0x4d, 0x56},                         // KDMV
	ImageFormatVHDX:  {0x76, 0x68, 0x64, 0x78, 0x66, 0x69, 0x6c, 0x65}, // vhdxfile
	ImageFormatVHD:   {0x63, 0x6f, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x78}, // connectix
	ImageFormatVDI:   {0x7f, 0x10, 0xda, 0xbe},
}

var formatCookieOffsets = map[ImageFormat]int{
	ImageFormatVDI: 0x40,
	// We want to recognize only VHD DYNAMIC format which has a copy of footer
	// at the beginning of the image.
	// VHD RAW format should be recognized as RAW format.
	ImageFormatVHD: 0,
}

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

	buffer := make([]byte, cookieBufferSize)
	_, err := reader.Read(ctx, 0, buffer)
	if err != nil {
		return "", err
	}

	for imageFormat := range formatCookies {
		ok := hasAtOffset(
			ctx,
			buffer,
			formatCookies[imageFormat],
			formatCookieOffsets[imageFormat],
		)

		if ok {
			return imageFormat, nil
		}
	}

	return ImageFormatRaw, nil
}

func hasAtOffset(
	ctx context.Context,
	buffer []byte,
	cookie []byte,
	offset int,
) bool {

	if offset+len(cookie) > len(buffer) {
		return false
	}

	return bytes.HasPrefix(buffer[offset:], cookie)
}
