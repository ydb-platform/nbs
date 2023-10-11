package vmdk

////////////////////////////////////////////////////////////////////////////////

const (
	// Marker types are necessary when reading stream-optimized image
	// sequentially.
	markerEOS            markerType = 0
	markerGrainTable     markerType = 1
	markerGrainDirectory markerType = 2
	markerFooter         markerType = 3
)

////////////////////////////////////////////////////////////////////////////////

type markerType uint8

type marker struct {
	Value    sectors
	DataSize uint32

	// If |DataSize > 0| then Type is populated by garbage, because marker is
	// followed by grain data.
	Type markerType
}

func (m marker) dataOffset() uint64 {
	// In case of data marker, marker ends after DataSize and Type is not stored.
	return 12
}
