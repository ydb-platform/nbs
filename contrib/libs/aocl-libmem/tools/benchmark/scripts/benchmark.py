#!/usr/bin/python3
"""
 Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.

 Redistribution and use in source and binary forms, with or without modification,
 are permitted provided that the following conditions are met:
 1. Redistributions of source code must retain the above copyright notice,
    this list of conditions and the following disclaimer.
 2. Redistributions in binary form must reproduce the above copyright notice,
    this list of conditions and the following disclaimer in the documentation
    and/or other materials provided with the distribution.
 3. Neither the name of the copyright holder nor the names of its contributors
    may be used to endorse or promote products derived from this software without
    specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.

 benchmark.py -- Entry point for the redesigned benchmark framework.
 Same CLI as bench.py, but uses gbench_main.cpp via NewGBM instead of gbench.cpp.

 Usage:
   ./benchmark.py gbm memcpy -x 98 -bestperf
   ./benchmark.py gbm strcmp -m c
   ./benchmark.py gbm memmove -o f
   ./benchmark.py gbm strlen -r 8 4096
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'external'))

from bench import main as bench_main


class NewBench:
    """Routes to NewGBM instead of GBM for the redesigned benchmark framework."""

    def __init__(self, **kwargs):
        self.ARGS = kwargs
        self.MYPARSER = self.ARGS["ARGS"]

    def __call__(self, *args, **kwargs):
        if self.MYPARSER['benchmark'] == 'gbm':
            from new_gbm import NewGBM
            gbm = NewGBM(ARGS=self.ARGS, class_obj=self)
            gbm()
        elif self.MYPARSER['benchmark'] == 'tbm':
            from tbm import TBM
            tbm = TBM(ARGS=self.ARGS, class_obj=self)
            tbm()
        elif self.MYPARSER['benchmark'] == 'fbm':
            from fbm import FBM
            fbm = FBM(ARGS=self.ARGS, class_obj=self)
            fbm()
        elif self.MYPARSER['benchmark'] == 'dcperf':
            from dcperf import DCPerf
            dcperf = DCPerf(MYPARSER=self.MYPARSER, ARGS=self.ARGS, class_obj=self)
            dcperf()
        else:
            print(f"Unknown benchmark: {self.MYPARSER['benchmark']}")
            sys.exit(1)


if __name__ == "__main__":
    import subprocess
    try:
        subprocess.check_output(['which', 'numactl'])
    except subprocess.CalledProcessError:
        print("numactl utility NOT found. Please install it.")
        sys.exit(1)

    myparser = bench_main()
    obj = NewBench(ARGS=myparser)
    obj()
