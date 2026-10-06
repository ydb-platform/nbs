# User Guide for _AOCL-LibMem_
**_AOCL-LibMem_**  is a Linux library of data movement and manipulation
functions (such as ``memcpy()`` and ``strcpy()``) highly optimized for AMD
**Zen** micro-architecture. It has multiple implementations of each
function, supporting AVX2, AVX512, and ERMS CPU features. The default
choice is the best-fit implementation based on the underlying
micro-architectural support for CPU features and instructions. It
also supports tunable build under which a specific implementation can
be chosen for ``mem*`` functions as per the application requirements
with respect to alignments, instruction choice, and threshold values
as tunable parameters.

This release of the AOCL-LibMem library supports the following
functions:

-  memcpy
-  mempcpy
-  memmove
-  memset
-  memcmp
-  memchr
-  strcpy
-  strncpy
-  strcmp
-  strncmp
-  strlen
-  strnlen
-  strcat
-  strncat
-  strstr
-  strspn
-  strchr

`Note:` _Behavior might be undefined if AVX512 is disabled in the BIOS configuration on the Zen4+ platform._

## 1. Building AOCL-LibMem for Linux
Refer to the [BUILD_RUN.md](../BUILD_RUN.md)

## 2. Running an Application

Run the application by either preloading the shared *libaocl-libmem.so* or by linking it with the static binary *libaocllibmem.a* generated from the aforementioned build procedure.

To run the application, preload the *libaocl-libmem.so* generated
from the build procedure above:

```
   $ LD_PRELOAD=<path to build/lib/libaocl-libmem.so> <executable> <params>
     <compiler> <program> -L<path to build/lib> -l:libaocl-libmem.a
```
`Note:`Compiler should be Linux-based, and the linking-order of the libraries are important for the symbol resolution of the libmem supported functions.<br>
Linking with the static library may offer improved performance when compared to shared binary. Therefore, for **optimal performance**, it is recommended to link with **static library**.

## 3. User Config Options
###  3.1 Native Config:
Best fit solution for the native machine will be chosen by the CMake build system and this option provides the
best possible performance for a given micro-architecture.

### 3.2 Dynamic Config:
The library will choose the best-fit implementation of the underlying hardware during the init-phase of runtime.
This option is viable for running the same binary(library) on a mixed fleet.

`Note:`Dynamic dispatching may introduce latencies that mostly impact small-sized operations.
### 3.3 ISA specific Config:
Building library for either AVX2 or AVX512 machines. This binary cannot guarantee micro-arch specific performance
improvements.

### 3.4 Tunable Config:
Tunable build option provides users with the option to either choose the instruction or tune the threshold values.

The default LibMem build generates a best optimized shared/static
library tuned for the under lying Zen-micro architecture. LibMem also
provides a Tunable build option where the users have an option to choose
the instruction or tune the threshold values. This tunable build helps
the user to run different configurations for any given workload and
choose a best fit options for their workload and system configurations.

User should build a tunable binary to make use of the supported
tunables.

`Note:` _The tunable build is for experimentation purpose only._

#### 3.4.1 Running an Application with Tunables

LibMem built with tunables enabled exposes two tunable parameters that
will help you select the implementation of your choice:

-  LIBMEM_OPERATION: Instruction based on alignment and cacheability
-  LIBMEM_THRESHOLD: The threshold for ERMS and Non-Temporal
   instructions

Following two states are possible with this library
based on the tunable settings:

-  Default State: None of the parameters is tuned.
-  Tuned State: One of the parameters is tuned with a valid option.

##### 3.4.1.1 Default State

In this state, none of the parameters are tuned; the library will pick
up the best implementation based on the underlying AMD "Zen"
micro-architecture.

Run the application by preloading the tunables enabled

``libaocl-libmem.so``:

```
   $ LD_PRELOAD=<path to build/lib/libaocl-libmem.so> <executable> <params>
```

##### 3.4.1.2 Tuned State

In this state, one of the parameters is tuned by the application at run
time. The library will choose the implementation based on the valid
tuned parameter at run time. Only one of the tunable can be set to a
valid set of format/options as described in the below table.

### LIBMEM_OPERATION

You can set the tunable LIBMEM_OPERATION as follows

```
   LIBMEM_OPERATION=<operations>,<source_alignment>,<destination_alignment>
```
Based on this option, the library chooses the best implementation based
on the combination of move instructions, alignment of the source and
destination addresses.

 ##### Valid options:
 * `<operations> = [avx2|avx512|erms]`
 * `<source_alignment> = [b|w|d|q|x|y|n]`
 * `<destination_alignmnet> = [b|w|d|q|x|y|n]`

Use the following table to select the right implementation for your
application:

Application Requirement | LIBMEM_OPERATION | Implementation | Instructions |Side effects
------------------------|------------------|----------------|--------------|------------
**vector unaligned `source` & `destination`**|**[avx2\|avx512],b,b**|*__<func>_[avx2\|avx512]_unaligned*| Load:VMOVDQU; Store:VMOVDQU| None
**vector aligned `source` and `destination`**|**[avx2\|avx512],y,y**|*__<func>_[avx2\|avx512]_aligned*| Load:VMOVDQA; Store:VMOVDQA| _**unaligned source &/ destination** address will lead to_ **_`CRASH`_**.
**vector aligned `source` and unaligned `destination`**|**[avx2\|avx512],y,[b\|w\|d\|q\|x]**|*__<func>_[avx2\|avx512]_aligned_load*| Load:VMOVDQA; Store:VMOVDQU| None.
**vector unaligned `source` and aligned `destination`**|**[avx2\|avx512],[b\|w\|d\|q\|x], y**|*__<func>_[avx2\|avx512]_aligned_store*| Load:VMOVDQU; Store:VMOVDQA| None.
**vector `non temporal load and store`**|**[avx2\|avx512],n,n**|*__<func>_[avx2\|avx512]_nt_load*| Load:VMOVNTDQA; Store:VMOVNTDQ| _**unaligned source & destination** address will lead to_ **_`CRASH`_**.
**vector `non temporal load`**|**[avx2\|avx512],n,[b\|w\|d\|q\|x\|y]**|*__<func>_[avx2\|avx512]_nt_load*| Load:VMOVNTDQA; Store:VMOVDQU| None.
**vector `non temporal store`**|**[avx2\|avx512],[b\|w\|d\|q\|x\|y],n**|*__<func>_[avx2\|avx512]_nt_store*| Load:VMOVDQU; Store:VMOVNTDQ| None.
**rep movs `unaligned source or destination`**|**erms,b,b**|*__<func>_erms_b_aligned*| REP MOVSB | None
**rep movs `word aligned source & destination`**|**erms,w,w**|*__<func>_erms_w_aligned*| REP MOVSW | **`Data Coruption or Crash`** if length is not multiple of WORD.
**rep movs `double word aligned source & destination`**|**erms,d,d**|*__<func>_erms_d_aligned*| REP MOVSD | **`Data Coruption or Crash`** if length is not multiple of DOUBLE WORD.
**rep movs `quad word aligned source & destination`**|**erms,q,q**|*__<func>_erms_q_aligned*| REP MOVSQ | **`Data Coruption or Crash`** if length is not multiple of QUAD WORD.


`Note:` _A best-fit solution for the underlying micro-architecture will be chosen if the tunable is in an invalid format._

For example, to use only avx2-based move operations with both unaligned
source and aligned destination addresses:

```
   $ LD_PRELOAD=<build/lib/libaocl-libmem.so> LIBMEM_OPERATION=avx2,b,y <executable>
```

 ### LIBMEM_THRESHOLD

You can set the tunable LIBMEM_THRESHOLD as follows:

```
   LIBMEM_THRESHOLD=<repmov_start_threshold>,<repmov_stop_threshold>,<nt_start_threshold>,<nt_stop_threshold>
```
Based on this option, the library will choose the implementation with
tuned threshold settings for supported instruction sets: {`vector, repmov, non-temporal`}.

 ##### Valid options:
 * `<repmov_start_threshold> = [0, +ve integers]`
 * `<repmov_stop_threshold> = [0, +ve integers, -1]`
 * `<nt_start_threshold> = [0, +ve integers]`
 * `<nt_stop_threshold> = [0, +ve integers, -1]`

 *Where, -1 refers to the maximum length..*

Refer to the below table for sample threshold settings.

LIBMEM_THRESHOLD|Vector range|Rep Mov range|Non-Temporal range
----------------|------------|-------------|------------------
0,2048,1048576,-1|(2049, 1048576)|[0,2048]|[1048576, <max value of unsigned long long>)
0,0,1048576,-1|[0,1048576)|[0,0]|[1048576,<max value of unsigned long long>)

`Note:` _A system configured threshold will be chosen if the tunable
   is in an invalid format._

For example, to use ``REP MOVE`` instructions for a range of 1KB to
2KB and non_temporal instructions for a range of 512 KB and above:

```
   $ LD_PRELOAD=<build/lib/libaocl-libmem.so> LIBMEM_THRESHOLD=1024,2048,524288,-1 <executable>
```