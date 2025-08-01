#!/bin/bash

desc="ENAMETOOLONG: The length of a component of a pathname is longer than NAME_MAX"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..4"

# works correctly with NAME_MAX - 1
expect 0 mkdir ${name255} 0755
expect 0 opendir ${name255}
expect 0 rmdir ${name255}

# ENAMETOOLONG: Component name too long
expect ENAMETOOLONG opendir ${name256}
