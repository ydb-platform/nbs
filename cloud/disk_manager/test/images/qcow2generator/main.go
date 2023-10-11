package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"time"
	"unsafe"

	"github.com/spf13/cobra"
)

////////////////////////////////////////////////////////////////////////////////

// MAGIC "QFI\xfb".
var qcow2Magic = []byte{0x51, 0x46, 0x49, 0xFB}

// Represents header of qcow2 image format.
type header struct {
	Magic                 uint32 //     [0:3] QCOW magic string ("QFI\xfb").
	Version               uint32 //     [4:7] Version number.
	BackingFileOffset     uint64 //    [8:15] Offset into the image file at which the backing file name is stored.
	BackingFileSize       uint32 //   [16:19] Length of the backing file name in bytes.
	ClusterBits           uint32 //   [20:23] Number of bits that are used for addressing an offset whithin a cluster.
	Size                  uint64 //   [24:31] Virtual disk size in bytes.
	CryptMethod           uint32 //   [32:35] Encryption method.
	L1Size                uint32 //   [36:39] Number of entries in the active L1 table.
	L1TableOffset         uint64 //   [40:47] Offset into the image file at which the active L1 table starts.
	RefcountTableOffset   uint64 //   [48:55] Offset into the image file at which the refcount table starts.
	RefcountTableClusters uint32 //   [56:59] Number of clusters that the refcount table occupies.
	NbSnapshots           uint32 //   [60:63] Number of snapshots contained in the image.
	SnapshotsOffset       uint64 //   [64:71] Offset into the image file at which the snapshot table starts.
	IncompatibleFeatures  uint64 //   [72:79] for version >= 3: Bitmask of incompatible feature.
	CompatibleFeatures    uint64 //   [80:87] for version >= 3: Bitmask of compatible feature.
	AutoclearFeatures     uint64 //   [88:95] for version >= 3: Bitmask of auto-clear feature.
	RefcountOrder         uint32 //   [96:99] for version >= 3: Describes the width of a reference count block entry.
	HeaderLength          uint32 // [100:103] for version >= 3: Length of the header structure in bytes.
}

////////////////////////////////////////////////////////////////////////////////

func generateInvalid(outputFilePath string) error {
	rand.Seed(time.Now().UnixNano())

	var h header
	headerSize := uint64(unsafe.Sizeof(h))

	h.Magic = binary.BigEndian.Uint32(qcow2Magic)
	h.Version = 2
	h.ClusterBits = 10
	h.Size = 1024
	h.L1Size = 1
	h.L1TableOffset = 1024

	var buf bytes.Buffer

	err := binary.Write(&buf, binary.BigEndian, h)
	if err != nil {
		return err
	}

	bytes := make([]byte, 1024-headerSize)
	buf.Write(bytes)

	l2TableOffset := uint64(2048)
	if rand.Intn(2) == 0 {
		// Use huge offset to emulate 'out of bounds' error.
		l2TableOffset = math.MaxUint64
	}

	err = binary.Write(&buf, binary.BigEndian, l2TableOffset)
	if err != nil {
		return err
	}

	bytes = make([]byte, 1024-8)
	buf.Write(bytes)

	// Use huge offset to emulate 'out of bounds' error.
	clusterOffset := uint64(math.MaxUint64)
	err = binary.Write(&buf, binary.BigEndian, clusterOffset)
	if err != nil {
		return err
	}

	bytes = make([]byte, 1024)
	buf.Write(bytes)

	return ioutil.WriteFile(outputFilePath, buf.Bytes(), 0644)
}

func generateFuzzing(outputFilePath string) error {
	rand.Seed(time.Now().UnixNano())

	var h header
	h.Magic = binary.BigEndian.Uint32(qcow2Magic)
	h.Version = 2
	h.ClusterBits = 10
	h.Size = 64 * 1024 * 1024 // 64 MiB
	h.L1Size = 16
	h.L1TableOffset = 1024

	var buf bytes.Buffer

	err := binary.Write(&buf, binary.BigEndian, h)
	if err != nil {
		return err
	}

	// 64 MiB.
	bytes := make([]byte, 64*1024*1024)
	rand.Read(bytes)
	buf.Write(bytes)

	return ioutil.WriteFile(outputFilePath, buf.Bytes(), 0644)
}

func generate(mode string, outputFilePath string) error {
	switch mode {
	case "invalid":
		return generateInvalid(outputFilePath)
	case "fuzzing":
		return generateFuzzing(outputFilePath)
	default:
		return fmt.Errorf("unsupported mode %v", mode)
	}
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var mode string
	var outputFilePath string

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate QCOW2 file",
		Run: func(cmd *cobra.Command, args []string) {
			err := generate(mode, outputFilePath)
			if err != nil {
				log.Fatalf("Failed to generate: %v", err)
			}
		},
	}

	cmd.Flags().StringVar(
		&mode,
		"mode",
		"",
		"Mode: possible values [fuzzing]",
	)
	if err := cmd.MarkFlagRequired("mode"); err != nil {
		log.Fatalf("Error setting flag mode as required: %v", err)
	}

	cmd.Flags().StringVar(
		&outputFilePath,
		"output-file-path",
		"",
		"Path to the output file",
	)
	if err := cmd.MarkFlagRequired("output-file-path"); err != nil {
		log.Fatalf("Error setting flag output-file-path as required: %v", err)
	}

	if err := cmd.Execute(); err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}
