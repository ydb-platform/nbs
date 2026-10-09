import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "check_tstring_pascal_methods.py"


class CheckTStringPascalMethodsTest(unittest.TestCase):
    def run_script(self, content: str, added_line: str) -> subprocess.CompletedProcess:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            subprocess.check_call(["git", "init", "-q"], cwd=repo)
            subprocess.check_call(
                ["git", "config", "user.email", "test@example.com"],
                cwd=repo,
            )
            subprocess.check_call(
                ["git", "config", "user.name", "Test User"],
                cwd=repo,
            )

            path = repo / "sample.cpp"
            path.write_text(content, encoding="utf-8")
            subprocess.check_call(["git", "add", "sample.cpp"], cwd=repo)
            subprocess.check_call(["git", "commit", "-qm", "base"], cwd=repo)

            updated = content + added_line
            path.write_text(updated, encoding="utf-8")
            subprocess.check_call(["git", "add", "sample.cpp"], cwd=repo)

            return subprocess.run(
                [sys.executable, str(SCRIPT), "--cached", "sample.cpp"],
                cwd=repo,
                capture_output=True,
                text=True,
            )

    def test_flags_tstring_size_on_added_line(self):
        result = self.run_script(
            "TString Data;\n",
            "    for (ui32 i = 0; i < Data.Size(); ++i) {}\n",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("use .size() instead of .Size()", result.stderr)

    def test_ignores_non_string_size(self):
        result = self.run_script(
            "struct TRange { ui64 Size() const; };\nTRange range;\n",
            "    auto n = range.Size();\n",
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)

    def test_flags_tstring_data(self):
        result = self.run_script(
            "TStringBuf buf;\n",
            "    const char* ptr = buf.Data();\n",
        )
        self.assertEqual(result.returncode, 1)
        self.assertIn("use .data() instead of .Data()", result.stderr)


if __name__ == "__main__":
    unittest.main()
