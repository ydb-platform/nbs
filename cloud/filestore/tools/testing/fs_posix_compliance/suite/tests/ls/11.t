#!/bin/sh
# Test ls special permission bits display - setuid, setgid, sticky bit

desc="ls -l correctly displays special permission bits (S/s for setuid/setgid, T/t for sticky)"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..32"

n0=`namegen`

# Setuid bit on non-executable file shows 'S'
expect 0 create ${n0} 0644
expect 0 chmod ${n0} 04644
output=`ls -l ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "-rwSr--r--"
expect 0 unlink ${n0}

# Setuid bit on executable file shows 's'
expect 0 create ${n0} 0755
expect 0 chmod ${n0} 04755
output=`ls -l ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "-rwsr-xr-x"
expect 0 unlink ${n0}

# Setuid bit with different permissions
expect 0 create ${n0} 0600
expect 0 chmod ${n0} 04600
output=`ls -l ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "-rwS------"
expect 0 unlink ${n0}

# Setgid bit on non-executable file shows 'S'
expect 0 create ${n0} 0644
expect 0 chmod ${n0} 02644
output=`ls -l ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "-rw-r-Sr--"
expect 0 unlink ${n0}

# Setgid bit on executable file shows 's'
expect 0 create ${n0} 0755
expect 0 chmod ${n0} 02755
output=`ls -l ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "-rwxr-sr-x"
expect 0 unlink ${n0}

# Setgid bit on directory
expect 0 mkdir ${n0} 0755
expect 0 chmod ${n0} 02755
output=`ls -ld ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "drwxr-sr-x"
expect 0 rmdir ${n0}

# Sticky bit on directory without search permission shows 'T'
expect 0 mkdir ${n0} 0666
expect 0 chmod ${n0} 01666
output=`ls -ld ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "drw-rw-rwT"
expect 0 rmdir ${n0}

# Sticky bit on directory with search permission shows 't'
expect 0 mkdir ${n0} 0777
expect 0 chmod ${n0} 01777
output=`ls -ld ${n0} 2>&1 | cut -c1-10`
test_check "$output" = "drwxrwxrwt"
expect 0 rmdir ${n0}
