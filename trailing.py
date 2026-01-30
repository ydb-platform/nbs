from pathlib import Path

for path in Path(".").rglob("**/.gitignore"):
    text = path.read_text()
    if text.endswith("\n"):
        continue
    text += "\n"
    path.write_text(text)
