// Package blobid provides utilities for parsing NKikimrProto TTLogoBlobID.
//
// TTLogoBlobID is encoded as three fixed64 fields (RawX1, RawX2, RawX3) with
// the following bit layout (little-endian):
//
//	RawX1: TabletID (64 bits)
//	RawX2: StepR1[0:24] | Generation[24:56] | Channel[56:64]
//	RawX3: PartId[0:4] | BlobSize[4:30] | CrcMode[30:32] | Cookie[32:56] | StepR2[56:64]
//
//	Step = (StepR1 << 8) | StepR2
//
// String format: [TabletID:Generation:Step:Channel:Cookie:BlobSize:PartId]
package blobid

import "fmt"

////////////////////////////////////////////////////////////////////////////////

type TLogoBlobID struct {
	TabletID   uint64
	Generation uint32
	Step       uint32
	Channel    uint8
	Cookie     uint32
	BlobSize   uint32
	PartID     uint8
}

func NewTLogoBlobID(rawX1, rawX2, rawX3 uint64) TLogoBlobID {
	stepR1 := rawX2 & 0xFFFFFF
	stepR2 := (rawX3 >> 56) & 0xFF
	return TLogoBlobID{
		TabletID:   rawX1,
		Generation: uint32((rawX2 >> 24) & 0xFFFFFFFF),
		Step:       uint32((stepR1 << 8) | stepR2),
		Channel:    uint8((rawX2 >> 56) & 0xFF),
		PartID:     uint8(rawX3 & 0xF),
		BlobSize:   uint32((rawX3 >> 4) & 0x3FFFFFF),
		Cookie:     uint32((rawX3 >> 32) & 0xFFFFFF),
	}
}

func (id TLogoBlobID) String() string {
	return fmt.Sprintf("[%d:%d:%d:%d:%d:%d:%d]",
		id.TabletID, id.Generation, id.Step, id.Channel,
		id.Cookie, id.BlobSize, id.PartID)
}
