import subprocess
from pathlib import Path


for path in subprocess.check_output(
    ["grep", "-r", "--include", "ya.make", "PROGRAM"]
    ).decode().splitlines():
    [ya_make_path, contents] = path.split(":")
    if not ya_make_path.endswith("ya.make"):
        continue
    if "contrib" in ya_make_path:
        continue
    if ya_make_path.startswith("build"):
        continue
    if ya_make_path.startswith("library"):
        continue
    if ya_make_path.startswith("vendor"):
        continue
    if ya_make_path.startswith("tools"):
        continue
    dir_path = ya_make_path.replace("/ya.make", "")
    directory = dir_path.split("/")[-1]
    name = ""
    # print(f"Processing {dir_path}")
    if "PROGRAM" in contents:
        if "PROGRAM(" not in contents:
            continue
        name = contents.split("PROGRAM(")[1].split(")")[0]
        if name == "":
            name = directory
    if name == "":
        continue
    dir_path = Path(dir_path)
    gitignore = dir_path / ".gitignore"
    gitignore_text = name
    if (gitignore).exists():
        if name in gitignore.read_text():
            continue
        gitignore_text = gitignore.read_text() + "\n" + name
    gitignore.write_text(gitignore_text)
