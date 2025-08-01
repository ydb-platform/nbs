#!/bin/sh

desc="ELOOP: A loop exists in symbolic links encountered during resolution of the dirname argument"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..6"

n0=`namegen`
n1=`namegen`

# ELOOP: Loop exists in symbolic links
expect 0 symlink ${n0} ${n1}
expect 0 symlink ${n1} ${n0}
expect ELOOP opendir ${n0}
expect ELOOP opendir ${n1}
expect 0 unlink ${n0}
expect 0 unlink ${n1}
