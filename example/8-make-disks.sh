#!/usr/bin/env bash
source ./prepare_binaries.sh || exit 1

suffix=${NBS_CELL_SUFFIX:-}
for arg in "$@"; do
    case "$arg" in
        --cell2)
            run_in_cell2 "$@"
            ;;
        -h|--help)
            echo "Usage: ./8-make-disks.sh [--cell2]"
            echo "Create the example disks in cell-1, or in cell-2 with a -cell2 suffix."
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

require_bins "$BLOCKSTORE_CLIENT_BIN" || exit 1

blockstore-client CreatePlacementGroup --group-id="pg-part${suffix}" --placement-strategy=partition --partition-count=4
blockstore-client CreatePlacementGroup --group-id="pg-spread${suffix}" --placement-strategy=spread

blockstore-client CreateVolume --disk-id="nrd1${suffix}" --storage-media-kind nonreplicated --blocks-count=262144 --block-size=4096 --cloud-id=cloud --folder-id=folder --placement-group-id="pg-spread${suffix}"
blockstore-client CreateVolume --disk-id="nrd2${suffix}" --storage-media-kind nonreplicated --blocks-count=262144 --block-size=8192 --cloud-id=cloud --folder-id=folder --placement-group-id="pg-part${suffix}" --placement-partition-index=1
blockstore-client CreateVolume --disk-id="mrr1${suffix}" --verbose error --storage-media-kind mirror3 --blocks-count=262144 --cloud-id=cloud --folder-id=folder
blockstore-client CreateVolume --disk-id="ssd1${suffix}" --verbose error --storage-media-kind ssd --blocks-count=262144 --cloud-id=cloud --folder-id=folder
blockstore-client CreateVolume --disk-id="ssd2${suffix}" --verbose error --storage-media-kind ssd --blocks-count=262144 --partitions-count=2 --cloud-id=cloud --folder-id=folder
blockstore-client CreateVolume --disk-id="hdd1${suffix}" --verbose error --storage-media-kind hybrid --blocks-count=262144 --cloud-id=cloud --folder-id=folder
blockstore-client CreateVolume --disk-id="lcl1${suffix}" --storage-media-kind local --blocks-count=262144 --cloud-id=cloud --folder-id=folder
blockstore-client CreateVolume --disk-id="lcl2${suffix}" --storage-media-kind local --blocks-count=262144 --cloud-id=cloud --folder-id=folder --encryption-mode=aes-xts --encryption-key-path=encryption-key.txt
