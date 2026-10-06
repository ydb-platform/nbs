
"""
 Copyright (C) 2023 Advanced Micro Devices, Inc. All rights reserved.

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
import numpy as np
from pandas import read_csv
import csv
import matplotlib.pyplot as plt
import os
from __main__ import bench_name, result_dir

"""Global Variables: result,func,graph_size_range"""
func=['memcpy']
barWidth = 0.1
GlibcVersion = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'\
        ", shell=True)
LibMemVersion = subprocess.check_output("file ../lib/shared/libaocl-libmem.so \
        | awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell =True)


def throughput_addlabels(x, y1, y2, g):
    """
    This function adds the %Gain Label to the Throughput Graph
    args:
        x:  Positon on the X-axis.
        y1: Position on the Y-axis(Glibc Throughput)
        y2: Position on the Y-axis(Amd Throughput)
        g:  Throughput Gain Value
    returns: none
    """
    for i in range(len(x)):
        #+VE Gains
        if g[i]>=0:
            plt.text(x[i], y2[i], str(g[i])+"%", fontstyle='normal', \
                    color='green', size=11, fontweight ='bold')
        #-VE Gains
        else:
            plt.text(x[i], y2[i], str(g[i])+"%", fontstyle='normal', \
                    color='red', size=11, fontweight ='bold')

def throughput_GraphPlot(a, b, br1, u):
    """
    This function Plots the Throughput Graph
    args:
        a: Amd-Throughput Value
        b: Glibc-Throughput Value
        br1: Position Marking on the X-axis
        u: Size List
    returns: none
    """
    barWidth = 0.1
    br2 = [x + barWidth for x in br1]
    plt.plot( br1, b, color ='#4472c4', label ='GLIBC-'+str(GlibcVersion, \
            'utf-8').strip(), marker='o',linestyle='dashed')
    plt.plot( br1, a, color ='#ed7d31', label ='LibMem-'+str(LibMemVersion,\
            'utf-8').strip(), marker='o',linestyle='dotted' )
    plt.xlabel('Size in Bytes', fontweight ='bold', fontsize = 10)
    if bench_name in ['TinyMemBench','GooglBench_Cached','GooglBench_\
            UnCached']:
        plt.ylabel('Throughput in MB/s', fontweight ='bold', fontsize = 10)
    else:
        plt.ylabel('Throughput in GB/s', fontweight ='bold', fontsize = 10)
    #for Plotting the 'SIZE' values
    plt.xticks(br1,u)

input_file = read_csv(result_dir+"/"+str(bench_name)+"throughput_values.csv")

#Converting the Column Values to LIST
size = input_file['Size'].values.tolist()
Th_amd= input_file['amd_Throughput'].values.tolist()
Th_glibc= input_file['glibc_Throughput'].values.tolist()
Th_gains= input_file['GAINS'].values.tolist()
Memlength = len(size)


#Converting the size to required unit
unit = []
for x in size:
    if x<1024:
        unit.append(str(x)+'B')
    elif x >= 1024 and x< (1024*1024):
        x = x//1024
        unit.append(str(x)+'KB')
    else:
        x = x//1048576
        unit.append(str(x)+'MB')
############################THROUGHPUT LINECHART GRAPH#####################
##################Figure Size for the 1st Graph (Default:8B-4KB) RANGE#####
fig = plt.subplots(figsize =(8, 5))
Memlength = len(size)
##Setting up the RANGE VALUES And dividing based on the input size parameter 10->7->Rest##
if Memlength >=10 or Memlength >=1:
    step = 0
    step = min(Memlength, 10)
# Reading only first 10 Values
    a = Th_amd[0:step]
    b = Th_glibc[0:step]
    g = Th_gains[0:step]
    u = unit[0:step]
    br1 = []
    for i in np.arange(0, 0.5*step, 0.5):
        br1.append(i)
    # Make the 1st Plot
    plt.title(str(bench_name)+" Report ["+str(u[0])+"-"+str(u[-1])+"] "+\
            str(func).upper(), fontweight="bold")
    throughput_GraphPlot(a, b, br1, u)

    #Adding the %Gain labels
    throughput_addlabels(br1, b, a, g)
    plt.legend(bbox_to_anchor=(0, 1.15), loc='upper center')
    plt.grid(True)
    #f1name="Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "
    plt.savefig(result_dir+"/"+str(bench_name)+" Report ["+str(u[0])+"-"\
            +str(u[-1])+"]"+".png")

    #Dividing the Next Set of Size Values
    Memlength = Memlength-step

    if Memlength >=7 or Memlength >=1:
        #####Figure Size for the 2nd Graph Subplot(Default:8KB-1MB) RANGE
        fig = plt.subplots(figsize =(10, 5))
        step = min(Memlength, 7)
        a = Th_amd[10:10+step]
        b = Th_glibc[10:10+step]
        g = Th_gains[10:10+step]
        u = unit[10:10+step]
        temp = u
        br1 = []
        for i in np.arange(0, 0.5*step, 0.5):
            br1.append(i)

        plt.subplot(1, 2, 1)

        plt.title(str(bench_name)+" Report ["+str(u[0])+"-"+str(u[-1])+"] "\
                +str(func).upper(), fontweight="bold")
        throughput_GraphPlot(a, b, br1, u)
        throughput_addlabels(br1, b, a, g)
        plt.legend()
        plt.grid(True)
        #Figure Size for the 2nd Graph Subplot(Default:1MB-64MB) RANGE###
        Memlength = Memlength-step
        if Memlength >0:
            a = Th_amd[17:]
            b = Th_glibc[17:]
            g = Th_gains[17:]
            u = unit[17:]
            br1 = []
            for i in np.arange(0, 0.5*Memlength, 0.5):
                br1.append(i)
            plt.subplot(1, 2, 2)
            plt.title(" ["+str(u[0])+"-"+str(u[-1])+"] "+str(func).upper(),\
                    fontweight="bold")
            throughput_GraphPlot(a, b, br1, u)
            throughput_addlabels(br1, b, a, g)
            plt.legend()
            plt.grid(True)
        #########2nd Graph SupTitle and Report Name#########################
        plt.savefig(result_dir+"/"+str(bench_name)+" Report ["+str(temp[0])\
                +"-"+str(u[-1])+"]"+".png")

