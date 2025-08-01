#!/bin/bash
# Test ls recursion and symlink handling - -R, -H, -L options

desc="ls -R recursively lists directories, -H and -L handle symbolic links"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..150"

n0=`namegen`
n1=`namegen`
n2=`namegen`

# ls -R lists subdirectory contents
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n0}/subdir 0755
expect 0 create ${n0}/file1 0644
expect 0 create ${n0}/subdir/file2 0644
output=`ls -R ${n0} 2>&1 | grep -E "file1|file2|subdir:" | wc -l`
test_check "$output" -eq 3
expect 0 unlink ${n0}/subdir/file2
expect 0 rmdir ${n0}/subdir
expect 0 unlink ${n0}/file1
expect 0 rmdir ${n0}

# ls -R with multiple levels
create_nested_dirs ${n0} level1 level2
expect 0 create ${n0}/file0 0644
expect 0 create ${n0}/level1/file1 0644
expect 0 create ${n0}/level1/level2/file2 0644
output=`ls -R ${n0} 2>&1 | grep -E "^${n0}:|level1:|level2:" | wc -l`
test_check "$output" -eq 3
expect 0 unlink ${n0}/level1/level2/file2
expect 0 unlink ${n0}/level1/file1
expect 0 unlink ${n0}/file0
rm_nested_dirs ${n0} level1 level2

# ls -R does not follow symlinks to directories by default
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n1}/file 0644
expect 0 symlink ${n1} ${n0}/symdir
output=`ls -R ${n0} 2>&1 | grep "file" | wc -l`
test_check "$output" -eq 0
expect 0 unlink ${n0}/symdir
expect 0 unlink ${n1}/file
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}

# ls -R on empty directory tree
create_nested_dirs ${n0} empty1 empty2
output=`ls -R ${n0} 2>&1 | grep -v "^$" | grep -v ":" | tr '\n' ' ' | sed 's/ $//'`
test_check_quoted "$output" = "empty1 empty2"
rm_nested_dirs ${n0} empty1 empty2

# ls -R with hidden directories
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n0}/.hidden 0755
expect 0 create ${n0}/.hidden/file 0644
output=`ls -R ${n0} 2>&1 | grep "file" | wc -l`
test_check "$output" -eq 0
expect 0 unlink ${n0}/.hidden/file
expect 0 rmdir ${n0}/.hidden
expect 0 rmdir ${n0}

# ls -Ra shows hidden directories
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n0}/.hidden 0755
expect 0 create ${n0}/.hidden/file 0644
output=`ls -Ra ${n0} 2>&1 | grep -E "\.hidden:|file" | wc -l`
test_check "$output" -eq 2
expect 0 unlink ${n0}/.hidden/file
expect 0 rmdir ${n0}/.hidden
expect 0 rmdir ${n0}

# ls -R format includes directory headers
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n0}/sub 0755
output=`ls -R ${n0} 2>&1 | grep -E "^${n0}:$|^${n0}/sub:$" | wc -l`
test_check "$output" -eq 2
expect 0 rmdir ${n0}/sub
expect 0 rmdir ${n0}

# ls follows symlinks to directories with -H when specified on command line
expect 0 mkdir ${n0} 0755
expect 0 create ${n0}/file 0644
expect 0 symlink ${n0} ${n1}
output=`ls -H ${n1} 2>&1`
test_check "$output" = "file"
expect 0 unlink ${n1}
expect 0 unlink ${n0}/file
expect 0 rmdir ${n0}

# ls does not follow symlinks in subdirectories with -H
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n1}/file 0644
expect 0 symlink ${n1} ${n0}/symdir
output=`ls -RH ${n0} 2>&1 | grep "file" | wc -l`
test_check "$output" -eq 0
expect 0 unlink ${n0}/symdir
expect 0 unlink ${n1}/file
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}

# ls -RL follows all symlinks
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n1}/file 0644
expect 0 symlink ${n1} ${n0}/symdir
output=`ls -RL ${n0} 2>&1 | grep "file" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}/symdir
expect 0 unlink ${n1}/file
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}

# ls -l shows symlink info without -L
expect 0 create ${n0} 0644
expect 0 symlink ${n0} ${n1}
output=`ls -l ${n1} 2>&1 | grep -E "${n1} -> ${n0}" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n1}
expect 0 unlink ${n0}

# ls -lL shows target file info
expect 0 create ${n0} 0644
echo "testtesttest" > ${n0}
expect 0 symlink ${n0} ${n1}
size1=`ls -lL ${n1} 2>&1 | awk '{print $5}'`
size2=`ls -l ${n0} 2>&1 | awk '{print $5}'`
test_check "$size1" = "$size2"
expect 0 unlink ${n1}
expect 0 unlink ${n0}

# ls -H with broken symlink
expect 0 symlink nonexistent ${n0}
output=`ls -H ${n0} 2>&1 | grep "${n0}" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}

# ls -L with broken symlink shows error
expect 0 symlink nonexistent ${n0}
ls -L ${n0} >/dev/null 2>&1
test_check "$?" -ne 0
expect 0 unlink ${n0}

# ls -R with -L follows symlinks recursively
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n1}/file 0644
expect 0 symlink "../${n1}" ${n0}/symdir
output=`ls -RL ${n0} 2>&1 | grep -E "symdir:|file" | wc -l`
test_check "$output" -eq 2
expect 0 unlink ${n0}/symdir
expect 0 unlink ${n1}/file
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}

# Symlink to file with -H
expect 0 create ${n0} 0644
expect 0 symlink ${n0} ${n1}
output=`ls -H ${n1} 2>&1`
test_check "$output" = "${n1}"
expect 0 unlink ${n1}
expect 0 unlink ${n0}

# Multiple symlinks with -L
expect 0 create target 0644
expect 0 symlink target link1
expect 0 symlink link1 link2
output=`ls -lL link2 2>&1 | grep -E "^-rw-r--r--" | wc -l`
test_check "$output" -eq 1
expect 0 unlink link2
expect 0 unlink link1
expect 0 unlink target

# ls -R depth handling
create_nested_dirs ${n0} a b c d e
output=`ls -R ${n0} 2>&1 | grep ":" | wc -l`
test_check "$output" -eq 5
rm_nested_dirs ${n0} a b c d e

# ls -R with special files
expect 0 mkdir ${n0} 0755
expect 0 mkfifo ${n0}/pipe 0644
output=`ls -R ${n0} 2>&1 | grep "pipe" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}/pipe
expect 0 rmdir ${n0}

# Combination of -R, -L, and -a
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 create ${n1}/.hidden 0644
expect 0 symlink "../${n1}" ${n0}/link
output=`ls -RLa ${n0} 2>&1 | grep ".hidden" | wc -l`
test_check "$output" -eq 1
expect 0 unlink ${n0}/link
expect 0 unlink ${n1}/.hidden
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}

# ls -R detects simple symlink loop
expect 0 mkdir ${n0} 0755
expect 0 symlink . ${n0}/loop
# Should detect loop and not hang
timeout 2 ls -R ${n0} >/dev/null 2>&1
test_check "$?" -eq 0
expect 0 unlink ${n0}/loop
expect 0 rmdir ${n0}

# ls -R detects mutual symlink loop
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n1} 0755
expect 0 symlink ${n1} ${n0}/to_n1
expect 0 symlink ${n0} ${n1}/to_n0
timeout 2 ls -R ${n0} >/dev/null 2>&1
test_check "$?" -eq 0
expect 0 unlink ${n0}/to_n1
expect 0 unlink ${n1}/to_n0
expect 0 rmdir ${n0}
expect 0 rmdir ${n1}

# ls -RL detects loops when following symlinks
expect 0 mkdir ${n0} 0755
expect 0 mkdir ${n0}/sub 0755
expect 0 symlink .. ${n0}/sub/parent
timeout 2 ls -RL ${n0} >/dev/null 2>&1
test_check "$?" -ne 0
expect 0 unlink ${n0}/sub/parent
expect 0 rmdir ${n0}/sub
expect 0 rmdir ${n0}

# ls -R handles deep directory structures without false positives
create_nested_dirs ${n0} a b c d e f g h i j
output=`ls -R ${n0} 2>&1 | grep -c ":"`
test_check "$output" -eq 10
rm_nested_dirs ${n0} a b c d e f g h i j

# ls -R handles too long directory names
create_too_long
ls -R ${name_max} >/dev/null 2>&1
test_check "$?" -eq 0
unlink_too_long

# ls -L with broken symlink cycles
expect 0 symlink ${n1} ${n0}
expect 0 symlink ${n0} ${n1}
timeout 2 ls -L ${n0} ${n1} >/dev/null 2>&1
test_check "$?" -ne 0
expect 0 unlink ${n0}
expect 0 unlink ${n1}
