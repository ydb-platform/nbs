import subprocess
import sys


subprocess.check_call("""
    mkdir x &&
    cd x &&
    ar x {} &&
    tar zxf data.tar.gz &&
    cp var/lib/nbs/libblockstore-plugin.so ..
    """.format(sys.argv[1]),
    shell=True,
)
