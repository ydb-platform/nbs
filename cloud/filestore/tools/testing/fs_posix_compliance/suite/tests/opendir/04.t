#!/bin/sh

desc="ENOENT: A component of dirname does not name an existing directory or dirname is an empty string"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..5"

n0=`namegen`

# ENOENT: Directory does not exist
expect ENOENT opendir "nonexistent_dir_12345"

# ENOENT: Current removed directory does exist only in relative path
cdir=`pwd`
expect 0 mkdir ${n0} 0755
cd ${n0}
expect 0 rmdir "../$n0"
expect 0 opendir .
expect ENOENT opendir "$cdir/$n0"
cd ${cdir}
