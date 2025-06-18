import os
import sys

def open_and_close_files(num_files):
    fds = []
    try:
        for i in range(num_files):
            path = f"testfile_{i}"
            # Create the file if it doesn't exist
            fd = os.open(path, os.O_RDWR | os.O_CREAT)
            print(f"Opened: {path} (fd: {fd})")
            fds.append(fd)
    except OSError as e:
        print(f"Error opening file: {e}")
    finally:
        for fd in fds:
            try:
                os.close(fd)
                print(f"Closed fd: {fd}")
            except OSError as e:
                print(f"Error closing fd {fd}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <number_of_files>")
        sys.exit(1)
    try:
        num = int(sys.argv[1])
        if num <= 0:
            raise ValueError
    except ValueError:
        print("Please provide a positive integer.")
        sys.exit(1)

    open_and_close_files(num)
