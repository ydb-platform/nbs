./5-create_disk.sh -k nonreplicated -d nrd1
#./blockstore-client.sh CreateVolume --disk-id=nrd1 --storage-media-kind nonreplicated --blocks-count=131072 --block-size=8192

./blockstore-client.sh ExecuteAction --action=modifytags --input-bytes='{"DiskId":"nrd1","TagsToAdd":"source-disk-id=nrd1-copy","TagsToRemove":""}'

./blockstore-client.sh createvolumelink --leader-disk-id=nrd1-copy --follower-disk-id=nrd1
