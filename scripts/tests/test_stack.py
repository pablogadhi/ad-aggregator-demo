"""python3 -m unittest discover scripts/tests   (stdlib only; stack.py brings its own pyyaml via uv)"""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

STACK = Path(__file__).parent.parent / "stack.py"


def run(yaml_text: str, *args: str) -> subprocess.CompletedProcess:
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        f.write(yaml_text)
    try:
        env = {**os.environ, "SDL_STACK_FILE": f.name}
        return subprocess.run(["python3", str(STACK), *args], env=env, capture_output=True, text=True)
    finally:
        os.unlink(f.name)


STACK_YAML = """
components:
  - postgres
  - name: postgres
    instance: analytics-db
    profile: ha
  - { name: kafka, profile: ha }
"""


class Entries(unittest.TestCase):
    def test_defaults_instance_and_profile(self):
        out = run(STACK_YAML, "entries").stdout.splitlines()
        self.assertEqual(out, ["postgres small postgres", "postgres ha analytics-db", "kafka ha kafka"])

    def test_filter_by_instance_or_name(self):
        self.assertEqual(run(STACK_YAML, "entries", "analytics-db").stdout.split(), ["postgres", "ha", "analytics-db"])
        self.assertEqual(len(run(STACK_YAML, "entries", "postgres").stdout.splitlines()), 2)

    def test_profile_by_instance(self):
        self.assertEqual(run(STACK_YAML, "profile", "analytics-db").stdout.strip(), "ha")
        self.assertEqual(run(STACK_YAML, "profile", "kafka").stdout.strip(), "ha")

    def test_duplicate_instance_is_invalid(self):
        res = run("components: [postgres, postgres]\n", "validate", "components")
        self.assertNotEqual(res.returncode, 0)
        self.assertIn("listed twice", res.stderr)


if __name__ == "__main__":
    unittest.main()
