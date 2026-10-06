"""
 Copyright (C) 2022 Advanced Micro Devices, Inc. All rights reserved.

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
from __main__ import result, func
import numpy as np
from pandas import read_csv
import csv
import matplotlib.pyplot as plt



"""Global Variables: result,func,graph_size_range"""

barWidth = 0.1
GlibcVersion = subprocess.check_output("ldd --version | awk '/ldd/{print $NF}'", shell=True)
LibMemVersion = subprocess.check_output("file ../lib/shared/libaocl-libmem.so | awk -F 'so.' '/libaocl-libmem.so/{print $3}'", shell =True)

def latency_addlabels(x, y1, y2, g):
    """
    This function adds the %Gain Label to the Latency Graph
    args:
        x: Positon on the X-axis.
        y1: Position on the Y-axis(Glibc)
        y2: Position on the Y-axis(Amd)
        g: Gain Value
    returns: none
    """
    for i in range(len(x)):
        #+VE Gains
        if g[i]>=0:
            plt.text(x[i], y2[i], str(g[i])+"%", fontstyle='normal', color='green', size=11, family='sans-serif', weight='bold')
        #-VE Gains
        else:
            plt.text(x[i], y2[i], str(g[i])+"%", fontstyle='normal', color='red', size=11, family='sans-serif', weight='bold')

def latency_GraphPlot(a, b, br1, u):
    """
    This function Plots the Latency Graph
    args:
        a: Amd-Latency Value
        b: Glibc-Latency Value
        br1: Position Marking on the X-axis
        u: Size List
    returns: none
    """
    barWidth = 0.1
    br2 = [x + barWidth for x in br1]
    plt.plot( br1, b, color ='#4472c4', label ='GLIBC-'+str(GlibcVersion, 'utf-8').strip(), marker='o',linestyle='dashed')
    plt.plot( br1, a, color ='#ed7d31', label ='LibMem-'+str(LibMemVersion, 'utf-8').strip(), marker='o',linestyle='dotted' )
    plt.xlabel('Size in Bytes', fontweight ='bold', fontsize = 10)
    plt.ylabel('Latency in ns', fontweight ='bold', fontsize = 10)
    #for Plotting the 'SIZE' values
    plt.xticks(br1, u)

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
            plt.text(x[i], y2[i], str(g[i])+"%", fontstyle='normal', color='green', size=11, fontweight ='bold')
        #-VE Gains
        else:
            plt.text(x[i], y2[i], str(g[i])+"%", fontstyle='normal', color='red', size=11, fontweight ='bold')

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
    plt.plot( br1, b, color ='#4472c4', label ='GLIBC-'+str(GlibcVersion, 'utf-8').strip(), marker='o',linestyle='dashed')
    plt.plot( br1, a, color ='#ed7d31', label ='LibMem-'+str(LibMemVersion, 'utf-8').strip(), marker='o',linestyle='dotted' )
    plt.xlabel('Size in Bytes', fontweight ='bold', fontsize = 10)
    plt.ylabel('Throughput in GB/s', fontweight ='bold', fontsize = 10)
    #for Plotting the 'SIZE' values
    plt.xticks(br1,u)

amd = read_csv(str(result)+"/amd_latency_report.csv")
glibc = read_csv(str(result)+"/glibc_latency_report.csv")
performance = read_csv(str(result)+"/perf_report.csv")

#Converting the Column Values to LIST
amd_latency = amd['latency'].values.tolist()
glibc_latency = glibc['latency'].values.tolist()
lat_gains = performance['performance gains(%)'].values.tolist()
size = amd['size'].values.tolist()
Th_amd= []
Th_glibc= []
Th_gains= []
Memlength = len(size)

#Converting Latency values(TSC) to Nanoseconds by dividing with ClockCycles
cpu_freq = subprocess.check_output("cat /sys/devices/system/cpu/cpu7/cpufreq/scaling_cur_freq ", shell=True)
cpu_freq = float(str(cpu_freq,'utf-8').strip()) / 10**6      # converts 1497221KHz-->(float conversion)1497221.0KHz--->1.4972210KHz
cpu_freq = float("{:.2f}".format(cpu_freq))                  #1.49 -> roundoff upto two decimals  1.5GHz

for x in range(len(amd_latency)):
        amd_latency[x] = amd_latency[x]/cpu_freq
        glibc_latency[x] = glibc_latency[x]/cpu_freq
        Th_amd.append(size[x]/amd_latency[x])
        Th_glibc.append(size[x]/glibc_latency[x])
        Th_gains.append( round(((Th_amd[x] - Th_glibc[x] )/ Th_glibc[x] )*100))

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
######################################################LATENCY LINECAHRT GRAPH########################################
##############################Figure Size for the 1st Graph (Default:8B-4KB) RANGE########################################
fig = plt.subplots(figsize =(8, 5))

##Setting up the RANGE VALUES And dividing based on the input size parameter 10->7->Rest##
if Memlength >=10 or Memlength >=1:
    step = 0
    step = min(Memlength, 10)
# Reading only first 10 Values
    a = amd_latency[0:step]
    b = glibc_latency[0:step]
    g = lat_gains[0:step]
    u = unit[0:step]
    br1 = []
    for i in np.arange(0, 0.5*step, 0.5):
        br1.append(i)
    # Make the 1st Plot
    plt.title("Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "+str(func).upper(), fontweight="bold")
    latency_GraphPlot(a, b, br1, u)

    #Adding the %Gain labels
    latency_addlabels(br1, b, a, g)
    plt.legend(bbox_to_anchor=(0, 1.15), loc='upper center')
    plt.grid(True)
    #f1name="Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "
    plt.savefig(str(result)+"/"+"Latency Linechart Report ["+str(u[0])+"-"+str(u[-1])+"]"+'.png')

    #Dividing the Next Set of Size Values
    Memlength = Memlength-step

    if Memlength >=7 or Memlength >=1:
        ##############################Figure Size for the 2nd Graph Subplot(Default:8KB-1MB) RANGE##########################
        fig = plt.subplots(figsize =(10, 5))
        step = min(Memlength, 7)
        a = amd_latency[10:10+step]
        b = glibc_latency[10:10+step]
        g = lat_gains[10:10+step]
        u = unit[10:10+step]
        temp = u
        br1 = []
        for i in np.arange(0, 0.5*step, 0.5):
            br1.append(i)

        plt.subplot(1, 2, 1)

        plt.title("Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "+str(func).upper(), fontweight="bold")
        latency_GraphPlot(a, b, br1, u)
        latency_addlabels(br1, b, a, g)
        plt.legend()
        plt.grid(True)
        ##############################Figure Size for the 2nd Graph Subplot(Default:1MB-64MB) RANGE##########################
        Memlength = Memlength-step
        if Memlength >0:
            a = amd_latency[17:]
            b = glibc_latency[17:]
            g = lat_gains[17:]
            u = unit[17:]
            br1 = []
            for i in np.arange(0, 0.5*Memlength, 0.5):
                br1.append(i)
            plt.subplot(1, 2, 2)
            plt.title("Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "+str(func).upper(), fontweight="bold")
            latency_GraphPlot(a, b, br1, u)
            latency_addlabels(br1, b, a, g)
            plt.legend()
            plt.grid(True)
        ###########################2nd Graph SupTitle and Report Name#########################
        #plt.suptitle("AMD vs GlbiC ")
        plt.savefig(str(result)+"/"+"Latency Linechart Report ["+str(temp[0])+"-"+str(u[-1])+"]"+'.png')

######################################################THROUGHPUT LINECHART GRAPH########################################
##############################Figure Size for the 1st Graph (Default:8B-4KB) RANGE########################################
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
    plt.title("Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "+str(func).upper(), fontweight="bold")
    throughput_GraphPlot(a, b, br1, u)

    #Adding the %Gain labels
    throughput_addlabels(br1, b, a, g)
    plt.legend(bbox_to_anchor=(0, 1.15), loc='upper center')
    plt.grid(True)
    #f1name="Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "
    plt.savefig(str(result)+"/"+"Throughput Linechart Report ["+str(u[0])+"-"+str(u[-1])+"]"+'.png')

    #Dividing the Next Set of Size Values
    Memlength = Memlength-step

    if Memlength >=7 or Memlength >=1:
        ##############################Figure Size for the 2nd Graph Subplot(Default:8KB-1MB) RANGE##########################
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

        plt.title("Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "+str(func).upper(), fontweight="bold")
        throughput_GraphPlot(a, b, br1, u)
        throughput_addlabels(br1, b, a, g)
        plt.legend()
        plt.grid(True)
        ##############################Figure Size for the 2nd Graph Subplot(Default:1MB-64MB) RANGE##########################
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
            plt.title("Performance Report ["+str(u[0])+"-"+str(u[-1])+"] "+str(func).upper(), fontweight="bold")
            throughput_GraphPlot(a, b, br1, u)
            throughput_addlabels(br1, b, a, g)
            plt.legend()
            plt.grid(True)
        ###########################2nd Graph SupTitle and Report Name#########################
        plt.savefig(str(result)+"/"+"Throughput Linechart Report ["+str(temp[0])+"-"+str(u[-1])+"]"+'.png')


