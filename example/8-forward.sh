./5-create_disk.sh -k nonreplicated -d nrd1-copy
#./blockstore-client.sh CreateVolume --disk-id=nrd1-copy --storage-media-kind nonreplicated --blocks-count=131072 --block-size=8192

./blockstore-client.sh ExecuteAction --action=modifytags --input-bytes='{"DiskId":"nrd1-copy","TagsToAdd":"source-disk-id=nrd1","TagsToRemove":""}'

./blockstore-client.sh createvolumelink --leader-disk-id=nrd1 --follower-disk-id=nrd1-copy
