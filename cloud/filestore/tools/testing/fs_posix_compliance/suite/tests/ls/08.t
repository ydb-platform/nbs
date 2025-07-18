#!/bin/sh
# Test ls -f option - unsorted listing with special behaviors

desc="ls -f lists entries unsorted and enables -a"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..5"

# ls -f implies -a (shows hidden files)
expect 0 create .hidden 0644
expect 0 create visible 0644
output=`ls -f 2>&1 | grep -E "\.hidden" | wc -l`
test_check "$output" -eq 1
expect 0 unlink .hidden
expect 0 unlink visible
