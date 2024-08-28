package vmdk

import (
	"context"
	"encoding/binary"
	"unsafe"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
)

// Implementation is based on two main sources of information:
// https://www.vmware.com/app/vmdk/?src=vmdk
// https://github.com/libyal/libvmdk/blob/main/documentation/VMWare%20Virtual%20Disk%20Format%20(VMDK).asciidoc

////////////////////////////////////////////////////////////////////////////////

func NewImageMapReader(
	ctx context.Context,
	reader common.Reader,
) (*ImageMapReader, error) {

	imageMapReader := ImageMapReader{
		reader: reader,
	}

	err := imageMapReader.readHeader(ctx)
	if err != nil {
		return nil, err
	}

	return &imageMapReader, nil
}

type ImageMapReader struct {
	reader common.Reader
	header header
}

func (r *ImageMapReader) Size() uint64 {
	return r.header.Capacity.bytes()
}

func (r *ImageMapReader) Header() header {
	return r.header
}

func (r *ImageMapReader) Read(
	ctx context.Context,
) ([]common.ImageMapItem, error) {

	items := make([]common.ImageMapItem, 0)

	dataPerGT := uint64(r.header.NumGTEsPerGT) * r.header.GrainSize.bytes()
	// size/dataPerGT rounded up.
	numberOfGDEs := (r.Size() + dataPerGT - 1) / dataPerGT

	grainDirectory, err := r.readUint32s(
		ctx,
		r.grainDirectoryOffset(),
		int(numberOfGDEs),
	)
	if err != nil {
		return nil, err
	}

	var itemOffset uint64
	var lastGrain *grain

	for _, gde := range grainDirectory {
		var grainTable []uint32

		if gde == 0 ||
			(gde == 1 &&
				r.header.Flags&flagUseZeroedGrainTableEntries != 0 &&
				r.header.Version >= 2) {

			grainTable = make([]uint32, r.header.NumGTEsPerGT)
		} else {
			grainTable, err = r.readUint32s(
				ctx,
				sectors(gde),
				int(r.header.NumGTEsPerGT),
			)
			if err != nil {
				return nil, err
			}
		}

		for _, gte := range grainTable {
			var newGrain grain
			if gte == 0 {
				newGrain = grain{
					start:  itemOffset,
					length: r.header.GrainSize.bytes(),
					zero:   true,
				}
			} else {
				newGrain, err = r.readGrain(
					ctx,
					itemOffset,
					sectors(gte).bytes(),
				)
				if err != nil {
					return nil, err
				}
			}

			itemOffset += r.header.GrainSize.bytes()

			if lastGrain == nil {
				lastGrain = &newGrain
				continue
			}

			if lastGrain.mergeable(newGrain) {
				lastGrain.merge(newGrain)
				continue
			}

			items = append(items, lastGrain.item())
			lastGrain = &newGrain
		}
	}

	if lastGrain != nil {
		items = append(items, lastGrain.item())
	}

	return items, nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *ImageMapReader) readHeader(ctx context.Context) error {
	err := r.readHeaderFromOffset(ctx, 0)
	if err != nil {
		return err
	}

	if r.header.GdOffset == gdReadFooter {
		// Read footer from the second to last sector of the image.
		// Footer takes precedence over header.
		footerOffset := r.reader.Size() - 2*sectorSize
		if err := r.readHeaderFromOffset(ctx, footerOffset); err != nil {
			return err
		}
	}

	return nil
}

func (r *ImageMapReader) readHeaderFromOffset(
	ctx context.Context,
	offset uint64,
) error {

	headerSize := uint64(unsafe.Sizeof(r.header))
	if r.reader.Size() < headerSize {
		return common.NewSourceInvalidError(
			"image size %v is less than header size %v",
			r.reader.Size(),
			headerSize,
		)
	}

	err := r.reader.ReadBinary(
		ctx,
		offset,
		headerSize,
		binary.LittleEndian,
		&r.header,
	)
	if err != nil {
		return err
	}

	if r.header.MagicNumber != sparseMagicNumber {
		return common.NewSourceInvalidError(
			"check vmdk magic: expected - %v, actual - %v",
			sparseMagicNumber,
			r.header.MagicNumber,
		)
	}

	if r.header.Version > 3 || r.header.Version < 1 {
		return common.NewSourceInvalidError(
			"unsupported vmdk version: %v",
			r.header.Version,
		)
	}

	if !r.header.validate() {
		return common.NewSourceInvalidError("vmdk header is corrupted")
	}

	return nil
}

func (r *ImageMapReader) grainsCompressed() bool {
	return r.header.Flags&flagDataHasMarkers != 0 &&
		r.header.Flags&flagDataCompressed != 0 &&
		r.header.CompressAlgorithm == compressionDeflate
}

func (r *ImageMapReader) grainDirectoryOffset() sectors {
	if r.header.Flags&flagUseRedundantGrainDirectory != 0 {
		return r.header.RgdOffset
	}

	return r.header.GdOffset
}

func (r *ImageMapReader) readMarker(
	ctx context.Context,
	offset uint64,
) (marker, error) {

	var marker marker
	markerSize := uint64(unsafe.Sizeof(marker))
	err := r.reader.ReadBinary(
		ctx,
		offset,
		markerSize,
		binary.LittleEndian,
		&marker,
	)
	return marker, err
}

func (r *ImageMapReader) readGrain(
	ctx context.Context,
	itemOffset uint64,
	grainOffset uint64,
) (grain, error) {

	g := grain{
		start:      itemOffset,
		length:     r.header.GrainSize.bytes(),
		zero:       false,
		compressed: r.grainsCompressed(),
		offset:     grainOffset,
	}

	if !g.compressed {
		return g, nil
	}

	marker, err := r.readMarker(ctx, grainOffset)
	if err != nil {
		return grain{}, err
	}

	if marker.DataSize == 0 {
		return grain{}, common.NewSourceInvalidError(
			"expected data marker, but got marker type %v",
			marker.Type,
		)
	}

	g.compressedSize = uint64(marker.DataSize)
	g.offset = grainOffset + marker.dataOffset()
	return g, nil
}

func (r *ImageMapReader) readUint32s(
	ctx context.Context,
	offset sectors,
	count int,
) ([]uint32, error) {

	result := make([]uint32, count)
	data := make([]byte, count*4)

	_, err := r.reader.Read(ctx, offset.bytes(), data)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(data); i += 4 {
		result[i/4] = binary.LittleEndian.Uint32(data[i : i+4])
	}

	return result, nil
}
