#!/usr/bin/env python3
"""Query stack.yaml from shell scripts.

  stack.py components          -> one component name per line (install order; may repeat)
  stack.py entries [filter]     -> "<name> <profile> <instance>" per line (install order);
                                  filter keeps entries whose instance or name equals it
  stack.py profile <component> -> profile for that component (default: small)
  stack.py services            -> one service name per line
  stack.py pipelines           -> one pipeline name per line
  stack.py client              -> "true" / "false"
  stack.py validate [what]     -> exits non-zero if stack.yaml references missing folders
                                  (what: all | components; default all)
"""

import os
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # fall back to an ephemeral uv env so no global install is needed
    os.execvp("uv", ["uv", "run", "--quiet", "--no-project", "--with", "pyyaml", "python3", *sys.argv])

ROOT = Path(__file__).resolve().parent.parent
STACK_FILE = Path(os.environ.get("SDL_STACK_FILE", ROOT / "stack.yaml"))  # override for tests


def load() -> dict:
    data = yaml.safe_load(STACK_FILE.read_text()) or {}
    comps = []
    for c in data.get("components") or []:
        c = dict(c) if isinstance(c, dict) else {"name": c}
        # `instance` lets one component be installed several times (e.g. two postgres clusters);
        # it names the instance's resources and its conn secret (<instance>-conn). Default: the name.
        c.setdefault("instance", c["name"])
        c.setdefault("profile", "small")
        comps.append(c)
    data["components"] = comps
    return data


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    data = load()
    cmd = argv[0]
    if cmd == "components":
        print("\n".join(c["name"] for c in data["components"]))
    elif cmd == "entries":
        want = argv[1] if len(argv) > 1 else None
        for c in data["components"]:
            if want is None or want in (c["name"], c["instance"]):
                print(c["name"], c["profile"], c["instance"])
    elif cmd == "profile":
        for c in data["components"]:
            if argv[1] in (c["name"], c["instance"]):
                print(c["profile"])
                return 0
        print("small")
    elif cmd in ("services", "pipelines"):
        print("\n".join(data.get(cmd) or []))
    elif cmd == "client":
        print("true" if data.get("client") else "false")
    elif cmd == "validate":
        only_components = len(argv) > 1 and argv[1] == "components"
        errors = []
        instances = [c["instance"] for c in data["components"]]
        for dup in sorted({i for i in instances if instances.count(i) > 1}):
            errors.append(f"component instance '{dup}' is listed twice (set a distinct `instance:`)")
        for c in data["components"]:
            if not (ROOT / "infra/components" / c["name"] / "install.sh").exists():
                errors.append(f"component '{c['name']}' has no infra/components/{c['name']}/install.sh")
        for s in [] if only_components else data.get("services") or []:
            if not (ROOT / "services" / s / "pyproject.toml").exists():
                errors.append(f"service '{s}' has no services/{s}/pyproject.toml")
        for p in [] if only_components else data.get("pipelines") or []:
            if not (ROOT / "pipelines" / p).is_dir():
                errors.append(f"pipeline '{p}' has no pipelines/{p}/")
        for e in errors:
            print(f"stack.yaml: {e}", file=sys.stderr)
        return 1 if errors else 0
    else:
        print(f"unknown command: {cmd}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
