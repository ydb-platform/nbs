#!/bin/sh
# Test ls file metadata display - -i, -s options, -k skipped

desc="ls -i shows inodes, -s shows size in blocks, -k sets block size to 1024"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..55"

n0=`namegen`
n1=`namegen`
n2=`namegen`

# ls -i shows inode numbers
expect 0 create ${n0} 0644
output=`ls -i ${n0} 2>&1 | awk '{print $1}' | grep -E "^[0-9]+$" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# ls -i shows different inodes for different files
expect 0 create ${n0} 0644
expect 0 create ${n1} 0644
inode1=`ls -i ${n0} 2>&1 | awk '{print $1}'`
inode2=`ls -i ${n1} 2>&1 | awk '{print $1}'`
test_check "$inode1" != "$inode2"
expect 0 unlink ${n0}
expect 0 unlink ${n1}

# ls -i shows same inode for hard links
expect 0 create ${n0} 0644
expect 0 link ${n0} ${n1}
inode1=`ls -i ${n0} 2>&1 | awk '{print $1}'`
inode2=`ls -i ${n1} 2>&1 | awk '{print $1}'`
test_check "$inode1" = "$inode2"
expect 0 unlink ${n0}
expect 0 unlink ${n1}

# ls -i with multiple files
expect 0 create file1 0644
expect 0 create file2 0644
expect 0 create file3 0644
output=`ls -i file[123] 2>&1 | awk '{print $1}' | grep -E "^[0-9]+$" | wc -l`
test_check "$output" -eq 3
expect 0 unlink file1
expect 0 unlink file2
expect 0 unlink file3

# ls -i format shows inode followed by space and filename
expect 0 create ${n0} 0644
output=`ls -i ${n0} 2>&1`
echo "$output" | grep -E "^[0-9]+ ${n0}" >/dev/null
test_check "$?" -eq 0
expect 0 unlink ${n0}

# ls -s shows file sizes in blocks
expect 0 create ${n0} 0644
output=`ls -s ${n0} 2>&1 | awk '{print $1}' | grep -E "^[0-9]+$" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# ls -s shows different sizes for different files
expect 0 create ${n0} 0644
for i in {1..1000}; do
    echo "This is line $i with some extra text to make it longer"
done > ${n1}
size1=`ls -s ${n0} 2>&1 | awk '{print $1}'`
size2=`ls -s ${n1} 2>&1 | awk '{print $1}'`
test_check "$size2" -ge "$size1"
expect 0 unlink ${n0}
expect 0 unlink ${n1}

# ls -s with multiple files shows sizes for each
expect 0 create file1 0644
expect 0 create file2 0644
echo "content" > file3
output=`ls -s file[123] 2>&1 | awk '{print $1}' | grep -E "^[0-9]+$" | wc -l`
test_check "$output" -eq 3
expect 0 unlink file1
expect 0 unlink file2
expect 0 unlink file3

# ls -s format shows blocks before filename
expect 0 create ${n0} 0644
output=`ls -s ${n0} 2>&1`
echo "$output" | grep -E "^[0-9]+ ${n0}" >/dev/null
test_check "$?" -eq 0
expect 0 unlink ${n0}

# ls -s shows total blocks for directory listing
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/file1 0644
expect 0 create ${n0}/file2 0644
output=`ls -s ${n0} 2>&1 | head -1 | grep -E "^total [0-9]+" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}/file1
expect 0 unlink ${n0}/file2
expect 0 rmdir ${n0}

# ls -si combines inode and size
expect 0 create ${n0} 0644
output=`ls -si ${n0} 2>&1`
# Should have inode, blocks, filename
fields=`echo "$output" | awk '{print NF}'`
test_check "$fields" -eq 3
expect 0 unlink ${n0}

# ls -sl combines size display with long format
expect 0 create ${n0} 0644
output=`ls -sl ${n0} 2>&1 | grep -E "^[0-9]+.*-rw-r--r--" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# Empty file size handling
expect 0 create ${n0} 0644
size=`ls -s ${n0} 2>&1 | awk '{print $1}'`
test_check "$size" -ge 0
expect 0 unlink ${n0}
