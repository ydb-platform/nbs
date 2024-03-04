package vhd

import "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"

////////////////////////////////////////////////////////////////////////////////

const (
	footerCookie = "conectix"
)

////////////////////////////////////////////////////////////////////////////////

// https://learn.microsoft.com/en-us/windows/win32/vstor/about-vhd
type footer struct {
	Cookie             [8]byte
	Features           uint32
	FileFormatVersion  uint32
	DataOffset         uint64
	Timestamp          uint32
	CreatorApplication uint32
	CreatorVersion     uint32
	CreatorHostOS      uint32
	OriginalSize       uint64 // Stores the size of the hard disk in bytes at creation time.
	CurrentSize        uint64 // Stores the current size of the hard disk in bytes.
	DiskGeometry       uint32
	DiskType           uint32
	Checksum           uint32
	UniqueID           [16]byte
	SavedState         byte
	Reserved           [427]byte
}

func (f footer) validate() error {
	if string(f.Cookie[:]) != footerCookie {
		return common.NewSourceInvalidError(
			"Failed to check vhd footer cookie: expected - %s, actual - %s",
			footerCookie,
			f.Cookie,
		)
	}

	return nil // TODO: Implement.
}
