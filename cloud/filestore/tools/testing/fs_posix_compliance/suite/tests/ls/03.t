#!/bin/sh
# Test ls -l option - long format listing

desc="ls -l displays long format listing with file details"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..38"

n0=`namegen`
n1=`namegen`
n2=`namegen`

# ls -l shows correct file permissions
expect 0 create ${n0} 0644
output=`ls -l ${n0} 2>&1`
perms=`echo "$output" | cut -c1-10`
test_check "$perms" = "-rw-r--r--"
expect 0 unlink ${n0}

# ls -l shows correct directory permissions
expect 0 mkdir ${n0} 0755
output=`ls -ld ${n0} 2>&1`
perms=`echo "$output" | cut -c1-10`
test_check "$perms" = "drwxr-xr-x"
expect 0 rmdir ${n0}

# ls -l shows correct number of links
expect 0 create ${n0} 0644
expect 0 link ${n0} ${n1}
output=`ls -l ${n0} 2>&1`
links=`echo "$output" | awk '{print $2}'`
test_check "$links" = "2"
expect 0 unlink ${n0}
expect 0 unlink ${n1}

# ls -l shows file size correctly
expect 0 create ${n0} 0644
echo "test content" > ${n0}
output=`ls -l ${n0} 2>&1`
size=`echo "$output" | awk '{print $5}'`
test_check "$size" = "13"
expect 0 unlink ${n0}

# ls -l shows symlink target correctly
# POSIX: If the file is a symbolic link, the pathname field shall be of the form:
# "%s -> %s", <pathname of link>, <contents of link>
expect 0 create ${n0} 0644
expect 0 symlink ${n0} ${n1}
output=`ls -l ${n1} 2>&1`
echo "$output" | grep -E "^l.*${n1} -> ${n0}" >/dev/null
test_check "$?" -eq 0
expect 0 unlink ${n1}
expect 0 unlink ${n0}

# ls -l with multiple files maintains format
expect 0 create file1 0644
expect 0 create file2 0755
output=`ls -l file[12] 2>&1`
lines=`echo "$output" | grep -E "^-[rwx-]{9}" | wc -l`
test_check "$lines" -eq 2
expect 0 unlink file1
expect 0 unlink file2

# ls -l shows total blocks for directory
# POSIX: If any of the -l options is specified, each list of files within the
# directory shall be preceded by a status line indicating the number of file
# system blocks occupied by files in the directory
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/file 0644
output=`ls -l ${n0} 2>&1`
echo "$output" | head -1 | grep -E "^total [0-9]+" >/dev/null
test_check "$?" -eq 0
expect 0 unlink ${n0}/file
expect 0 rmdir ${n0}

# ls -l handles special characters in filenames TODO: create fails
# expect 0 create "file with spaces" 0644
# output=`ls -l "file with spaces" 2>&1`
# echo "$output" | grep "file with spaces" >/dev/null
# test_check "$?" -eq 0
# expect 0 unlink "file with spaces"

# ls -l shows executable bit correctly
expect 0 create ${n0} 0755
output=`ls -l ${n0} 2>&1`
perms=`echo "$output" | cut -c1-10`
test_check "$perms" = "-rwxr-xr-x"
expect 0 unlink ${n0}

# ls -l on empty file shows zero size
expect 0 create ${n0} 0644
output=`ls -l ${n0} 2>&1`
size=`echo "$output" | awk '{print $5}'`
test_check "$size" = "0"
expect 0 unlink ${n0}

# ls -l follows POSIX format specification exactly
expect 0 create ${n0} 0644
output=`ls -l ${n0} 2>&1`
# Verify format: permissions(10) links owner group size date name
echo "$output" | grep -E "^-[rwxSTst-]{9}[[:space:]]+[0-9]+[[:space:]]+[[:alnum:]_-]+[[:space:]]+[[:alnum:]_-]+[[:space:]]+[0-9]+[[:space:]]+[[:alpha:]]{3}[[:space:]]+[0-9]+[[:space:]]+([0-9]{2}:[0-9]{2}|[[:space:]]*[0-9]{4})[[:space:]]+${n0}" >/dev/null
test_check "$?" -eq 0
expect 0 unlink ${n0} 
