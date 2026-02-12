./blockstore-client.sh CreatePlacementGroup --group-id=pg-part --placement-strategy=partition --partition-count=4
./blockstore-client.sh CreatePlacementGroup --group-id=pg-spread --placement-strategy=spread

./blockstore-client.sh CreateVolume --disk-id=nrd1 --storage-media-kind nonreplicated --blocks-count=131072 --block-size=8192 --cloud-id=cloud --folder-id=folder --placement-group-id=pg-spread
./blockstore-client.sh CreateVolume --disk-id=nrd2 --storage-media-kind nonreplicated --blocks-count=262144 --block-size=4096 --cloud-id=cloud --folder-id=folder --placement-group-id=pg-part --placement-partition-index=1
./blockstore-client.sh CreateVolume --disk-id=mrr1 --verbose error --storage-media-kind mirror3 --blocks-count=262144 --cloud-id=cloud --folder-id=folder
./blockstore-client.sh CreateVolume --disk-id=ssd1 --verbose error --storage-media-kind ssd --blocks-count=262144 --cloud-id=cloud --folder-id=folder
./blockstore-client.sh CreateVolume --disk-id=ssd2 --verbose error --storage-media-kind ssd --blocks-count=262144 --partitions-count=2 --cloud-id=cloud --folder-id=folder
./blockstore-client.sh CreateVolume --disk-id=hdd1 --verbose error --storage-media-kind hybrid --blocks-count=262144 --cloud-id=cloud --folder-id=folder
