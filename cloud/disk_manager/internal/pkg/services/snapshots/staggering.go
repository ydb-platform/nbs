package snapshots

import (
	"crypto/sha256"
	"encoding/binary"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

// window must be non-negative: it is validated by NewService.
func snapshotStartOffset(
	idempotencyKey string,
	snapshotID string,
	zoneID string,
	diskID string,
	window time.Duration,
) time.Duration {

	if window == 0 {
		return 0
	}

	var data []byte
	var length [8]byte

	for _, value := range []string{
		idempotencyKey,
		snapshotID,
		zoneID,
		diskID,
	} {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))

		data = append(data, length[:]...)
		data = append(data, value...)
	}

	digest := sha256.Sum256(data)
	hash := binary.BigEndian.Uint64(digest[:8])

	return time.Duration(hash % uint64(window))
}
