package testcommon

import (
	"bytes"
	"context"
	"hash/crc32"
	"math/rand"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

func NewContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func FillDisk(
	nbsClient nbs.Client,
	diskID string,
	diskSize uint64,
) (uint32, uint64, error) {

	return FillEncryptedDisk(nbsClient, diskID, diskSize, nil)
}

func FillEncryptedDisk(
	nbsClient nbs.Client,
	diskID string,
	diskSize uint64,
	encryption *types.EncryptionDesc,
) (uint32, uint64, error) {

	ctx := NewContext()

	session, err := nbsClient.MountRW(
		ctx,
		diskID,
		0, // fillGeneration
		0, // fillSeqNumber
		encryption,
	)
	if err != nil {
		return 0, 0, err
	}
	defer session.Close(ctx)

	chunkSize := uint64(1024 * 4096) // 4 MiB
	blockSize := uint64(session.BlockSize())
	blocksInChunk := uint32(chunkSize / blockSize)
	acc := crc32.NewIEEE()
	storageSize := uint64(0)
	zeroes := make([]byte, chunkSize)

	rand.Seed(time.Now().UnixNano())

	for offset := uint64(0); offset < diskSize; offset += chunkSize {
		blockIndex := offset / blockSize
		data := make([]byte, chunkSize)
		dice := rand.Intn(3)

		var err error
		switch dice {
		case 0:
			rand.Read(data)
			if bytes.Equal(data, zeroes) {
				logging.Debug(ctx, "rand generated all zeroes")
			}

			err = session.Write(ctx, blockIndex, data)
			storageSize += chunkSize
		case 1:
			err = session.Zero(ctx, blockIndex, blocksInChunk)
		}
		if err != nil {
			return 0, 0, err
		}

		_, err = acc.Write(data)
		if err != nil {
			return 0, 0, err
		}
	}

	return acc.Sum32(), storageSize, nil
}

func GoWriteRandomBlocksToNbsDisk(
	ctx context.Context,
	nbsClient nbs.Client,
	diskID string,
) (func() error, error) {

	sessionCtx := NewContext()
	session, err := nbsClient.MountRW(
		sessionCtx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	if err != nil {
		return nil, err
	}

	errGroup := new(errgroup.Group)

	errGroup.Go(func() error {
		defer session.Close(sessionCtx)

		writeCount := uint32(1000)

		blockSize := session.BlockSize()
		blocksCount := session.BlockCount()
		zeroes := make([]byte, blockSize)

		rand.Seed(time.Now().UnixNano())

		for i := uint32(0); i < writeCount; i++ {
			blockIndex := uint64(rand.Int63n(int64(blocksCount)))
			dice := rand.Intn(2)

			var err error

			switch dice {
			case 0:
				data := make([]byte, blockSize)
				rand.Read(data)
				if bytes.Equal(data, zeroes) {
					logging.Debug(ctx, "rand generated all zeroes")
				}

				err = session.Write(ctx, blockIndex, data)
			case 1:
				err = session.Zero(ctx, blockIndex, 1)
			}

			if err != nil {
				return err
			}
		}

		return nil
	})

	return errGroup.Wait, nil
}
