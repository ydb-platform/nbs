import shutil
import sys

if len(sys.argv) != 3:
    print("Usage: python cp.py <src> <dst>")
    sys.exit(1)

src = sys.argv[1]
dst = sys.argv[2]

try:
    shutil.copy2(src, dst)
    print("Symlink created.")
except Exception as e:
    print(f"Failed: {e}")
