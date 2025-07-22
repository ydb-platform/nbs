#!/bin/sh

desc="ENOTDIR: A component of dirname names an existing file that is neither a directory nor a symbolic link to a directory"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..6"

n0=`namegen`
n1=`namegen`

# ENOTDIR: Component names existing file that is not a directory
expect 0 create ${n0} 0644
expect ENOTDIR opendir ${n0}
expect 0 unlink ${n0}

# ENOTDIR: Component in path names file that is not a directory
expect 0 create ${n0} 0644
expect ENOTDIR opendir ${n0}/${n1}
expect 0 unlink ${n0}
