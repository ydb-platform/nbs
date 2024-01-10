#!/bin/bash

set -e

function list_filestores {
  filestore-client listfilestores
}

function get_filesystem_config {
  filestore-client describe --filesystem $1 --verbose debug 2>&1
}

function resize_filesystem {
  filestore-client resize --filesystem $1 --blocks-count $2
}

filesystems=$(list_filestores)
echo "Filesystems: $filesystems"

for fs in $filesystems;
do
  prev_config=$(get_filesystem_config $fs)
  echo "$prev_config"
  prev_block_count=$(echo "$prev_config" | grep BlocksCount | awk '{print $2}')
  echo "BlocksCount: $prev_block_count"
  new_block_count=$prev_block_count
  echo "PrefferedBlocksCount: $new_block_count"

  resize_filesystem $fs $new_block_count

  current_config=$(get_filesystem_config $fs)
  echo "$current_config"
  current_block_count=$(echo "$current_config" | grep BlocksCount | awk '{print $2}')
  echo "BlocksCount: $current_block_count"

  if [[ $new_block_count -ne $current_block_count ]];
  then
    echo "Something went wrong"
    exit 1
  else
    echo "Everyting is ok"
    read -n1 -r -p "Press any key to continue..."
  fi
done
