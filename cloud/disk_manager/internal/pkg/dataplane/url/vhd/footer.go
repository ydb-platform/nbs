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
	Features           uint32    // A bit field used to indicate specific feature support.
	FileFormatVersion  uint32    // Is divided into a major/minor version and matches the version of the specification used in creating the file.
	DataOffset         uint64    // Absolute byte offset, from the beginning of the file, to the next structure.
	Timestamp          uint32    // Creation time of a hard disk image.
	CreatorApplication [4]byte   // Used to document which application created the hard disk.
	CreatorVersion     uint32    // Major/minor version of the application that created the hard disk image.
	CreatorHostOS      uint32    // Type of host operating system this disk image is created on.
	OriginalSize       uint64    // Size of the hard disk in bytes at creation time.
	CurrentSize        uint64    // Current size of the hard disk in bytes.
	DiskGeometry       uint32    // Cylinder, heads, and sectors per track value for the hard disk.
	DiskType           uint32    // Type of the disk.
	Checksum           uint32    // A basic checksum of the hard disk footer.
	UniqueID           [16]byte  // А unique ID stored in the hard disk which is used to identify the hard disk.
	SavedState         byte      // A one-byte flag that describes whether the system is in saved state.
	Reserved           [427]byte
}

func (f footer) validate() error {
	if string(f.Cookie[:]) != footerCookie {
		return common.NewSourceInvalidError(
			"failed to check vhd footer cookie: expected %s, actual %s",
			footerCookie,
			f.Cookie,
		)
	}

	if f.FileFormatVersion != fileFormatVersion {
		return common.NewSourceInvalidError(
			"failed to check vhd file format version: expected %v, actual %v",
			fileFormatVersion,
			f.FileFormatVersion,
		)
	}

	if f.DiskType != dynamicHardDiskType {
		return common.NewSourceInvalidError(
			"failed to check vhd disk type: expected %v, actual %v",
			dynamicHardDiskType,
			f.DiskType,
		)
	}

	return nil
}
