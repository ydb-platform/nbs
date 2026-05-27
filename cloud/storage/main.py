

def wrong_solution(arr: list[int]) -> [int, int]:
    if len(arr) < 1:
        return (-1, -1)
    if len(arr) == 1:
        return (0, 0)
    lastIncreasingElIndex: int = 0
    lastDecreasingElIndex: int = 0
    maxIncreasingElIndex: int = 0
    maxDecreasingElIndex: int = 0
    maxIncreasingLength: int = 0
    maxDecreasingLength: int = 0
    for i in range(1, len(arr) - 1):
        if arr[i - 1] >= arr[i]:
            if maxIncreasingLength < i - lastIncreasingElIndex:
                maxIncreasingLength = i - lastIncreasingElIndex
                maxIncreasingElIndex = lastIncreasingElIndex
            lastIncreasingElIndex = i
        if arr[i - 1] <= arr[i]:
            if maxDecreasingLength < i - lastDecreasingElIndex:
                maxDecreasingLength = i - lastDecreasingElIndex
                maxDecreasingElIndex = lastDecreasingElIndex
            lastDecreasingElIndex = i
    if maxDecreasingLength > maxIncreasingLength:
        return (maxDecreasingElIndex, maxDecreasingElIndex + maxDecreasingLength - 1)
    else:
        return (maxIncreasingElIndex, maxIncreasingElIndex + maxIncreasingLength - 1)


def longest
