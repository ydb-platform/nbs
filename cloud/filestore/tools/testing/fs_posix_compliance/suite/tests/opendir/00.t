#!/bin/bash

desc="basic opendir tests"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..13"

n0=`namegen`
n1=`namegen`
n2=`namegen`

# opens a directory
expect 0 mkdir ${n0} 0755
expect 0 opendir ${n0}
expect 0 rmdir ${n0}

# opens a hidden directory
expect 0 mkdir ".hidden_dir" 0755
expect 0 opendir ".hidden_dir"
expect 0 rmdir ".hidden_dir"

# opens a symlink to a directory
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n0}/${n1} 0755
expect 0 symlink ${n0}/${n1} ${n2}
expect 0 opendir ${n2}
expect 0 unlink ${n2}
expect 0 rmdir ${n0}/${n1}
expect 0 rmdir ${n0}
