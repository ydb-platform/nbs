# Testing Guide for **_AOCL-LibMem Validator Tools_**

## Description
- AOCL-LibMem Validator Framework is used for functional validation of LibMem supported functions.It performs data validation check , which checks the data integrity of the source and destination memory.

- Users can perform validation testing using any of the methods mentioned below:
   1. Testing with Validator script (build_dir/test $validator.py)
   2. Testing with ctest utility (build_dir/test $ctest)

## Debug Mode for LibMem Validator

The libmem validator supports a debug mode that provides detailed information about the testing process, including vector sizes, function names, and alignment combinations being tested.

### Enabling Debug Mode

To build the validator with debug mode enabled, use the CMake option:

```bash
# Navigate to your build directory
cd /path/to/build/directory

# Configure with debug mode enabled
cmake -DLIBMEM_VALIDATOR_DEBUG=ON .

# Build the project
make
```

### Disabling Debug Mode

To build the validator without debug output (normal mode):

```bash
# Navigate to your build directory
cd /path/to/build/directory

# Configure with debug mode disabled
cmake -DLIBMEM_VALIDATOR_DEBUG=OFF .

# Build the project
make
```

### Debug Output Information

When debug mode is enabled, the validator will output the following information:

- **VEC_SZ**: Vector size in bytes (32 for AVX2, 64 for AVX512)
- **Function name**: Which function is being tested
- **Test size**: The size parameter being validated
- **Alignment mode**: Whether testing single alignment or all alignment combinations
- **Individual alignment tests**: Each source/destination alignment combination (when using alignment check mode)

### Example Debug Output

```
[DEBUG] libmem_validator started
[DEBUG] VEC_SZ = 64 bytes
[DEBUG] Function: memcpy
[DEBUG] Size: 1024
[DEBUG] Alignment check mode: All alignments
[DEBUG] Testing alignment - src: 0, dst: 0
[DEBUG] Testing alignment - src: 0, dst: 1
[DEBUG] Testing alignment - src: 0, dst: 2
...
[DEBUG] Testing alignment - src: 63, dst: 63
```

### Usage Examples

```bash
# Test with debug output enabled
./tools/validator/libmem_validator memcpy 1024 0 0 1

# Test without debug output (clean run)
./tools/validator/libmem_validator memcpy 1024 0 0 0
```

**Note**: Debug mode is used only for debugging validator code.

## 1. Validator script:

- By default the tool checks for validation of standard sizes with source and destination alignment as cache line SIZE . However , for non - standard sizes the user can pass the size range  along with the -t <iterator> value for validating the target sizes with different combinations of source and destination alignment.
 - For the given Input SIZE the tool shows the Validation result as
Passed / Failed.


## Report Description
  - Results will be generated in the format of csv file.
  - Validation report will be stored in this PATH
```
build_dir/test/out/<libmem_function>/<time-stamp-counter>/<validation_report.csv>
```

## Arguments for the framework


    $ ./validator.py -r [start] [end] -a[src] [dst] -t[iterator count] <mem_function>
        -r Range       = [Start] and [End] range in Bytes.
        -a alignment   = [src] and [dst] alignments. Default alignment is 64B for both source and destination.
        -t <iterator>  = specify the iteration pattern. Default is "2x" of starting size - '<<1'
        <mem_function> = memcpy,memset,memcmp,memmove,mempcpy,strcpy,strncpy

## Example
        Sample Run: $./validator.py -r 8 64 -a 5 8 -t"+1" memcpy
        Perfroms the Validation testing starting from 8 Bytes --> 64 Bytes
        with Src Alignment: 5 & Dst Alignment: 8
        and iterator pattern as "+1".

        > Validation for size [8] in progress...
        > Validation for size [9] in progress...
        > Validation for size [10] in progress...
        ...



The tester can provide different options as needed. Please refer to the help message
of the python script to set up  the validator config.
```sh
   $ ./validator.py -h
```

## 2. Ctest utility :
LibMem ctest for unit testing and validation of LibMem functions.It can be invoked from BUILD_DIR/test directory.

The following three test categories are supported:
- Iterator testing (increments by 1B till Page_Size)  [0B-4KB]
- Shift testing (Tests for standard sizes)  [8B,16B,32B...32MB]
- Alignment testing (Modifies the src and dst alignment)  [1B-4KB]   src [0-63]   dst [0-63]

## Report Description
  - ctest validation report will be stored in this PATH
        ```
        build_dir/test/Testing/Temporary/LastTest.log
        ```
  - storing the test logs with Timestamp
  ```sh
   $ ctest -R "memcpy_shift" -O ctest_results_$(date +%Y_%m_%d_%H%M%S).log
```
## Arguments for the framework

    $ ctest -R <Matching test regex> -E <Exclude regex> -j [<jobs>]
    <regrex>:     function_[MODE]
                  "memcpy","memset","memmove","memcmp", "mempcpy"
                  "strcpy","strncpy","strcmp","strncmp","strlen"   (Runs all the test cases iter,shift and alignment)
                  "iter"                                 (Runs Iterator test cases on all the functions)
                  "shift"                                (Runs Shift test cases on all the functions)
                  "align"                                (Runs Alignment test cases on all the functions)
                  "threshold"                            (Runs threshold specific test cases based on L2 and threshold_nt values)
                  "aligned_vector"                       (Runs ZEN5 specific aligned vector threshold checks)
                  "function_MODE"                        (Runs the specified MODE:iter,shift,align, and threshold on the input function)

    jobs:        Optional paramter for maximum no. of concurrent processes to be used. [0...$(nproc)]

## For Minimized logs
Storing only failure logs
```sh
   $ ctest --progress --output-on-failure -j $(nproc)
```
## Examples

1. Running ctest for all modes and all functions (default config)
```sh
   $ ctest
```
2. Running ctest for memcpy all modes
```sh
   $ ctest -R "memcpy" -j $(nproc)
```
3. Running ctest for memcmp align mode
```sh
   $ ctest -R "memcmp_align" -j $(nproc)
```
4. Running ctest for memmove iterative mode
```sh
   $ ctest -R "memmove_iter" -j $(nproc)
```
5. Running ctest for memset shift mode
```sh
   $ ctest -R "memset_shift" -j $(nproc)
```
6. Running with multiple inputs
```sh
   $ ctest -R "memcpy|memmove|strcpy_iter"
```
7. Running threshold checks
```sh
   $ ctest -R "memcpy_threshold"
```
8. Using Exclude regex
```sh
   $ ctest -R "memcpy|strcpy" -E "iter|shift"   //Runs memcpy_align and strcpy_align ;ignores iter and shift test cases.
```
9. Test log repots with timestamp
```sh
   $ ctest -R "memcpy_shift" -O ctest_results_$(date +%Y_%m_%d_%H%M%S).log
```

Notes:
1. Running ctest on Memcmp, Strcmp and Strncmp takes some extra time (as we are trying to compare all the Bytes).
