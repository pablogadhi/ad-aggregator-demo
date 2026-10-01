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


class Flux(unittest.TestCase):
    """stack.py flux: the Kustomizations Flux reconciles (uses the real infra/components/ folders)."""

    def kustomizations(self, text: str) -> dict:
        import yaml  # available: stack.py re-execs through uv when missing, tests run where it's installed
        docs = yaml.safe_load_all(run(text, "flux").stdout)
        return {d["metadata"]["name"]: d["spec"] for d in docs if d}

    def test_operator_once_then_one_per_instance(self):
        ks = self.kustomizations(STACK_YAML)
        self.assertEqual(list(ks)[:4], ["postgres-operator", "postgres", "analytics-db", "kafka-operator"])
        self.assertEqual(ks["analytics-db"]["dependsOn"], [{"name": "postgres-operator"}])
        self.assertEqual(ks["analytics-db"]["postBuild"]["substitute"], {"INSTANCE": "analytics-db", "PROFILE": "ha"})
        self.assertEqual(ks["postgres"]["path"], "./components/postgres/flux/instance")

    def test_profile_overlay_path_and_design_last(self):
        ks = self.kustomizations(STACK_YAML)
        self.assertEqual(ks["kafka"]["path"], "./components/kafka/flux/ha")
        if "design" in ks:
            self.assertEqual({d["name"] for d in ks["design"]["dependsOn"]}, {"postgres", "analytics-db", "kafka"})

    def test_validate_profile_and_single_instance(self):
        self.assertIn("no profile 'huge'", run("components: [{name: redis, profile: huge}]\n", "validate", "components").stderr)
        self.assertIn("one instance only", run("components: [{name: kafka, instance: k2}]\n", "validate", "components").stderr)
        self.assertEqual(run("components: [{name: flink, profile: small}]\n", "validate", "components").returncode, 0)
        self.assertEqual(run("components: [{name: redis, instance: cache, profile: ha}]\n", "validate", "components").returncode, 0)


if __name__ == "__main__":
    unittest.main()
