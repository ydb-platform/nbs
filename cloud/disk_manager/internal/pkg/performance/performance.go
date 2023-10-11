package performance

import (
	"time"
)

////////////////////////////////////////////////////////////////////////////////

const (
	MiB = uint64(1 << 20)
)

////////////////////////////////////////////////////////////////////////////////

func Estimate(dataSize, bandwidthMiBs uint64) time.Duration {
	bandwidthBytes := ConvertMiBsToBytes(bandwidthMiBs)
	return time.Duration(dataSize/bandwidthBytes) * time.Second
}

func ConvertMiBsToBytes(bandwidthMiBs uint64) uint64 {
	return bandwidthMiBs * MiB
}
