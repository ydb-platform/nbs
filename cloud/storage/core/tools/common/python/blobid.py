"""Utilities for parsing NKikimrProto.TTLogoBlobID.

TTLogoBlobID is encoded as three fixed64 fields (RawX1, RawX2, RawX3) with
the following bit layout (little-endian):

  RawX1: TabletID (64 bits)
  RawX2: StepR1[0:24] | Generation[24:56] | Channel[56:64]
  RawX3: PartId[0:4] | BlobSize[4:30] | CrcMode[30:32] | Cookie[32:56] | StepR2[56:64]

  Step = (StepR1 << 8) | StepR2

String format: [TabletID:Generation:Step:Channel:Cookie:BlobSize:PartId]
"""


class TLogoBlobID:
    def __init__(self, tabletId, generation, step, channel, cookie, blobSize, partId):
        self.tabletId = tabletId
        self.generation = generation
        self.step = step
        self.channel = channel
        self.cookie = cookie
        self.blobSize = blobSize
        self.partId = partId

    @classmethod
    def fromRaw(cls, rawX1, rawX2, rawX3):
        """Construct from three raw fixed64 values (RawX1, RawX2, RawX3)."""
        stepR1 = rawX2 & 0xFFFFFF
        generation = (rawX2 >> 24) & 0xFFFFFFFF
        channel = (rawX2 >> 56) & 0xFF
        partId = rawX3 & 0xF
        blobSize = (rawX3 >> 4) & 0x3FFFFFF
        cookie = (rawX3 >> 32) & 0xFFFFFF
        stepR2 = (rawX3 >> 56) & 0xFF
        step = (stepR1 << 8) | stepR2
        return cls(rawX1, generation, step, channel, cookie, blobSize, partId)

    @classmethod
    def fromProto(cls, proto):
        """Construct from a NKikimrProto.TTLogoBlobID proto message."""
        return cls.fromRaw(proto.RawX1, proto.RawX2, proto.RawX3)

    def __str__(self):
        return "[{}:{}:{}:{}:{}:{}:{}]".format(
            self.tabletId, self.generation, self.step,
            self.channel, self.cookie, self.blobSize, self.partId,
        )

    def __repr__(self):
        return str(self)
