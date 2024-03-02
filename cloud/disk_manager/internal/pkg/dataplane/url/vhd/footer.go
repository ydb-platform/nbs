package vhd

import "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"

////////////////////////////////////////////////////////////////////////////////

const (
	footerCookie        = "conectix"
	fileFormatVersion   = uint32(0x00010000)
	dynamicHardDiskType = uint32(3)
)

////////////////////////////////////////////////////////////////////////////////

// https://learn.microsoft.com/en-us/windows/win32/vstor/about-vhd
type footer struct {
	Cookie             [8]byte
	Features           uint32    // This is a bit field used to indicate specific feature support.
	FileFormatVersion  uint32    // This field is divided into a major/minor version and matches the version of the specification used in creating the file.
	DataOffset         uint64    // This field holds the absolute byte offset, from the beginning of the file, to the next structure.
	Timestamp          uint32    // This field stores the creation time of a hard disk image.
	CreatorApplication [4]byte   // This field is used to document which application created the hard disk.
	CreatorVersion     uint32    // This field holds the major/minor version of the application that created the hard disk image.
	CreatorHostOS      uint32    // This field stores the type of host operating system this disk image is created on.
	OriginalSize       uint64    // Stores the size of the hard disk in bytes at creation time.
	CurrentSize        uint64    // Stores the current size of the hard disk in bytes.
	DiskGeometry       uint32    // This field stores the cylinder, heads, and sectors per track value for the hard disk.
	DiskType           uint32    // Type of the disk.
	Checksum           uint32    // This field holds a basic checksum of the hard disk footer.
	UniqueID           [16]byte  // А unique ID stored in the hard disk which is used to identify the hard disk.
	SavedState         byte      // This field holds a one-byte flag that describes whether the system is in saved state.
	Reserved           [427]byte // This field contains zeroes.
}

func (f footer) validate() error {
	if string(f.Cookie[:]) != footerCookie {
		return common.NewSourceInvalidError(
			"Failed to check vhd footer cookie: expected - %s, actual - %s",
			footerCookie,
			f.Cookie,
		)
	}

	if f.FileFormatVersion != fileFormatVersion {
		return common.NewSourceInvalidError(
			"Failed to check vhd file format version: expected - %v, actual - %v",
			fileFormatVersion,
			f.FileFormatVersion,
		)
	}

	if f.DiskType != dynamicHardDiskType {
		return common.NewSourceInvalidError(
			"Failed to check vhd disk type: expected - %v, actual - %v",
			dynamicHardDiskType,
			f.DiskType,
		)
	}

	return nil
}
