package vhd

import "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"

////////////////////////////////////////////////////////////////////////////////

const (
	// The dynamic disk header should appear on a sector (512-byte) boundary.
	headerOffset  = uint64(512)
	headerCookie  = "cxsparse"
	batOffset     = uint64(1536)
	headerVersion = uint32(0x00010000)
)

// https://learn.microsoft.com/en-us/windows/win32/vstor/about-vhd
type header struct {
	Cookie               [8]byte
	DataOffset           uint64    // This field contains the absolute byte offset to the next structure in the hard disk image.
	TableOffset          uint64    // This field stores the absolute byte offset of the Block Allocation Table (BAT) in the file.
	HeaderVersion        uint32    // This field stores the version of the dynamic disk header.
	MaxTableEntries      uint32    // This field holds the maximum entries present in the BAT.
	BlockSize            uint32    // A block is a unit of expansion for dynamic and differencing hard disks.
	Checksum             uint32    // This field holds a basic checksum of the dynamic header.
	ParentUniqueID       [16]byte  // This field is used for differencing hard disks. Not supported.
	ParentTimeStamp      uint32    // This field is used for differencing hard disks. Not supported.
	Reserved1            uint32    // This field should be set to zero.
	ParentUnicodeName    [512]byte // This field is used for differencing hard disks. Not supported.
	ParentLocatorEntries [192]byte // This field is used for differencing hard disks. Not supported.
	Reserved2            [256]byte // This field should be set to zero.
}

func (h header) validate() error {
	if string(h.Cookie[:]) != headerCookie {
		return common.NewSourceInvalidError(
			"Failed to check vhd header cookie: expected - %s, actual - %s",
			headerCookie,
			h.Cookie,
		)
	}

	if h.TableOffset != batOffset {
		return common.NewSourceInvalidError(
			"Failed to check vhd header cookie: expected - %v, actual - %v",
			batOffset,
			h.TableOffset,
		)
	}

	if h.HeaderVersion != headerVersion {
		return common.NewSourceInvalidError(
			"Failed to check vhd header version: expected - %v, actual - %v",
			headerVersion,
			h.HeaderVersion,
		)
	}

	return nil
}
