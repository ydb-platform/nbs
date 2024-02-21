package vhd

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
	UniqueId           [16]byte
	SavedState         byte
	Reserved           [427]byte
}

func (h footer) validate() bool {
	return true // TODO: Implement.
}
