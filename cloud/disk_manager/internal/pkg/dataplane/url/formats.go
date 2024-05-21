package url

import (
	"bytes"
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/vhd"
)

//////////////////////////////////////////////////////////////////////////////

var formatCookies = map[ImageFormat][]byte{
	ImageFormatQCOW2: {0x51, 0x46, 0x49, 0xFB},                         // QFI\xfb
	ImageFormatVMDK:  {0x4b, 0x44, 0x4d, 0x56},                         // KDMV
	ImageFormatVHDX:  {0x76, 0x68, 0x64, 0x78, 0x66, 0x69, 0x6c, 0x65}, // vhdxfile
	ImageFormatVHD:   {0x63, 0x6f, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x78}, // connectix
	ImageFormatVDI:   {0x7f, 0x10, 0xda, 0xbe},
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

	for imageFormat := range formatCookies {
		offset := 0
		switch imageFormat {
		case ImageFormatVHD:
			offset = int(reader.Size() - vhd.FooterSize)
		case ImageFormatVDI:
			offset = 0x40
		}

		ok, err := hasAtOffset(
			ctx,
			reader,
			formatCookies[imageFormat],
			offset,
		)
		if err != nil {
			return "", err
		}

		if ok {
			return imageFormat, nil
		}
	}

	return ImageFormatRaw, nil
}

func hasAtOffset(
	ctx context.Context,
	reader common.Reader,
	cookie []byte,
	offset int,
) (bool, error) {

	if offset+len(cookie) > int(reader.Size()) {
		return false, nil
	}

	buffer := make([]byte, len(cookie))
	_, err := reader.Read(ctx, uint64(offset), buffer)
	if err != nil {
		return false, err
	}

	return bytes.Equal(buffer, cookie), nil
}
