package vhd

////////////////////////////////////////////////////////////////////////////////

const (
	headerCookie = "cxsparse"
)

// https://learn.microsoft.com/en-us/windows/win32/vstor/about-vhd
type header struct {
	Cookie               [8]byte
	DataOffset           uint64
	TableOffset          uint64
	HeaderVersion        uint32
	MaxTableEntries      uint32
	BlockSize            uint32
	Checksum             uint32
	ParentUniqueID       [16]byte
	ParentTimeStamp      uint32
	Reserved1            uint32
	ParentUnicodeName    [512]byte
	ParentLocatorEntries [192]byte
	Reserved2            [256]byte
}

func (h header) validate() bool {
	return true // TODO: Implement.
}
