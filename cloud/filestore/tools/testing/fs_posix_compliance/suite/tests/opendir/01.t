#!/bin/bash

desc="EACCESS: Search permission is denied for the component of the path prefix of dirname or read permission is denied for dirname."

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..14"

n0=`namegen`
n1=`namegen`

# Search permission is denied for the component of the path prefix of dirname
expect 0 mkdir ${n0} 0755
expect 0 chown ${n0} 65534 65534
expect 0 mkdir ${n0}/${n1} 0755
expect 0 chmod ${n0} 0644
expect EACCES -u 65534 -g 65534 opendir ${n0}/${n1}
expect 0 chmod ${n0} 0755
expect 0 rmdir ${n0}/${n1}
expect 0 rmdir ${n0}

# Read permission is denied for dirname
expect 0 mkdir ${n0} 0755
expect 0 chown ${n0} 65534 65534
expect 0 chmod ${n0} 000
expect EACCES -u 65534 -g 65534 opendir ${n0}
expect 0 chmod ${n0} 0755
expect 0 rmdir ${n0}
