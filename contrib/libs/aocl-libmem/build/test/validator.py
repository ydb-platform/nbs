#!/usr/bin/python3
"""
 Copyright (C) 2023-25 Advanced Micro Devices, Inc. All rights reserved.

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
"""


import subprocess
import os
from os import environ as env
import csv
import argparse
import datetime
from libmem_defs import *

class Libmem_Validator:
    def __init__(self, args):
       self.libmem_func = str(args.func)
       self.start_size = args.range[0]
       self.end_size = args.range[1]
       self.src_align = str(args.align[0])
       self.dst_align = str(args.align[1])
       self.iterator = str(args.iterator)

def data_validator(mem_func, size_range, alignment, iterator):
    """
    This function validates the data integrity of a string
    function.
    args:
        mem_func: memory function name.
        size_range: data size range.
        alignment: src and dst alignment.
        iterator: expression for iterating over data range.
    returns: string
        'Success' / 'Failure'
    """
    size = size_range[0]
    src_align = str(alignment[0])
    dst_align = str(alignment[1])
    print('>>> Data validaton of [amd '+mem_func+'] for size_range: ['\
          +str(size_range[0])+"-"+str(size_range[1])+'] with src_alignment = '\
          +src_align+', dst_alignment = '+dst_align)

    env['LD_PRELOAD'] = LIBMEM_BIN_PATH

    validation_status = 'Success'
    with open("validation_report.csv", 'a+') as v_result:
        if (size == 0) and (iterator == '<<1'):
            print('   > Validation for size ['+str(size)+'] in progress...')
            subprocess.run(["../tools/validator/libmem_validator", mem_func, str(size),\
                    src_align, dst_align], stdout=v_result, env = env, check=True)
            size += 1
        while size <= size_range[1]:
            print('   > Validation for size ['+str(size)+'] in progress...')
            subprocess.run(["../tools/validator/libmem_validator", mem_func, str(size),\
                    src_align, dst_align], stdout=v_result, env = env, check=True)
            size = eval('size'+' '+ iterator)
        print(v_result.read())
    if 'ERROR' in open("validation_report.csv", 'r').read():
        validation_status = 'Failure'

    return validation_status


def whole_number(value):
    try:
        value = int(value)
        if value < 0:
            raise argparse.ArgumentTypeError("{} is not a whole " \
                                                "number".format(value))
    except ValueError:
        raise Exception("{} is not whole number".format(value))
    return value

def main():
    libmem_funcs = ['memcpy', 'mempcpy', 'memmove', 'memset', 'memcmp',
                    'strcpy','strncpy','strcmp','strncmp', 'strlen', 'memchr', 'strcat', 'strncat', 'strstr', 'strspn', 'strchr']

    parser = argparse.ArgumentParser(prog='libmem_validator', description='This\
                            program will perform data validation of\
                            a memory function for a given range of data \
                            lengths and alignments.')
    parser.add_argument("func", help = "memory function to be verified",
                            type = str, choices = libmem_funcs)
    parser.add_argument("-r", "--range", nargs = 2, help="range of data\
                                lengths to be verified.",
                            type = whole_number, default = [8, 64 * 1024 * 1024])
    parser.add_argument("-a", "--align", nargs = 2, help = "alignemnt of source\
                                and destination addresses. Default alignment\
                                is 64B for both source and destination.",
                            type = whole_number, default = (64, 64))
    parser.add_argument("-t", "--iterator", help = "iteration pattern for a \
                            given range of data sizes. Default expression\
                            is set to 2x of starting size - '<<1'.",
                            type = str, default = '<<1')

    args = parser.parse_args()
    lbm = Libmem_Validator(args)
    test_result = 'Validation Summary:\n'
    test_status = 'Test Status : '
    status = True

    subprocess.call(['rm *.csv'], shell = True)
    # Analyse performance

    env['LD_PRELOAD'] = ''

    # Validate data
    validation = data_validator(args.func, args.range, args.align, args.iterator)
    if validation == 'Failure':
        status = False
    test_result += ' * Data Validation '+ validation +\
            '. Refer to <validation_report.csv> for more details.\n'


    result_dir = 'out/' + args.func + '/' + \
                datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
    os.makedirs(result_dir, exist_ok=False)
    subprocess.run(['mv','validation_report.csv',result_dir])
    #generate test configuration
    test_config = 'libmem_function :'+args.func+'\ndata_range[start, end]: '+\
            str(args.range)+'\nalignment(src, dst) :'+str(args.align)+\
            '\niterator : '+str(args.iterator)

    with open(result_dir+'/test.config', 'a+') as config_file:
        config_file.write(test_config)

    print(">>> Completed running all tests.\n")

    if status is True:
        test_status += 'SUCCESS\n'
    else:
        test_status += 'FAILURE\n'

    print(test_status,test_result)

    print("\n*** Test reports copied to directory ["+result_dir+"] ***\n")

    with open(result_dir+'/test_summary.log', 'a+') as summary:
        summary.write(test_status)
        summary.write(test_result)


if __name__=="__main__":
    main()
