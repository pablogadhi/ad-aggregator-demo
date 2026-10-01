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
  stack.py flux                -> Flux Kustomizations for the components + design (YAML), written to
                                  infra/flux/clusters/sdl/stack.generated.yaml by scripts/flux.sh
  stack.py flux-names          -> names of every Flux Kustomization the lab should end up with
"""

import os
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # fall back to an ephemeral uv env so no global install is needed
    os.execvp("uv", ["uv", "run", "--quiet", "--no-project", "--with", "pyyaml", "python3", *sys.argv])

ROOT = Path(__file__).resolve().parent.parent
INFRA = ROOT / "infra"

# Readiness of CRs that kstatus can't judge (it ignores a plain `Ready` condition). Added to every
# generated Kustomization; kinds a Kustomization doesn't apply are simply unused.
HEALTH_EXPRS = [
    {"apiVersion": "kafka.strimzi.io/v1", "kind": kind,
     "current": "status.conditions.exists(e, e.type == 'Ready' && e.status == 'True')"}
    for kind in ("Kafka", "KafkaTopic")
]
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


def component_dir(name: str) -> Path:
    return INFRA / "components" / name


def instance_path(c: dict) -> Path:
    """flux/<profile>/ when the profiles differ in plain YAML (an overlay), else flux/instance/."""
    d = component_dir(c["name"]) / "flux"
    return d / c["profile"] if (d / c["profile"]).is_dir() else d / "instance"


def kustomization(name: str, path: Path, depends: list[str], substitute: dict | None = None,
                  interval: str = "10m", timeout: str = "15m") -> dict:
    spec = {
        "dependsOn": [{"name": d} for d in depends],
        "interval": interval,
        "retryInterval": "1m",
        "timeout": timeout,
        "sourceRef": {"kind": "OCIRepository", "name": "flux-system"},
        "path": "./" + str(path.relative_to(INFRA)),
        "prune": True,
        "wait": True,
        "healthCheckExprs": HEALTH_EXPRS,
    }
    if substitute:
        spec["postBuild"] = {"substitute": substitute}
    return {"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization",
            "metadata": {"name": name, "namespace": "flux-system"}, "spec": spec}


def flux_kustomizations(data: dict) -> list[dict]:
    """platform-configs -> <component>-operator (once per component, if flux/operator/ exists)
    -> <instance> (one per stack.yaml entry, INSTANCE/PROFILE substituted) -> design."""
    docs, instances, seen = [], [], set()
    for c in data["components"]:
        op = component_dir(c["name"]) / "flux" / "operator"
        if c["name"] not in seen and op.is_dir():
            docs.append(kustomization(f"{c['name']}-operator", op, ["platform-configs"]))
        seen.add(c["name"])
        dep = f"{c['name']}-operator" if op.is_dir() else "platform-configs"
        # interval 2m: conn secrets and CRs deleted by hand come back quickly (drift demo)
        docs.append(kustomization(c["instance"], instance_path(c), [dep],
                                  {"INSTANCE": c["instance"], "PROFILE": c["profile"]}, interval="2m"))
        instances.append(c["instance"])
    design = INFRA / "design" / "flux"
    if (design / "kustomization.yaml").is_file():
        docs.append(kustomization("design", design, instances or ["platform-configs"], interval="2m"))
    return docs


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
            d = component_dir(c["name"])
            if not (d / "flux").is_dir():
                errors.append(f"component '{c['name']}' has no infra/components/{c['name']}/flux/")
                continue
            # the default profile is always valid: a component with a single profile ships only flux/instance/
            if c["profile"] != "small" and not ((d / "flux" / c["profile"]).is_dir() or (d / "values" / f"{c['profile']}.yaml").is_file()):
                errors.append(f"component '{c['name']}' has no profile '{c['profile']}'"
                              f" (flux/{c['profile']}/ or values/{c['profile']}.yaml)")
            if not instance_path(c).is_dir():
                errors.append(f"component '{c['name']}' has no flux/instance/ or flux/{c['profile']}/")
            single = d / "flux" / "single-instance"
            if single.exists() and c["instance"] != c["name"]:
                errors.append(f"component '{c['name']}' supports one instance only, named '{c['name']}'"
                              f" (got '{c['instance']}')")
        for s in [] if only_components else data.get("services") or []:
            if not (ROOT / "services" / s / "pyproject.toml").exists():
                errors.append(f"service '{s}' has no services/{s}/pyproject.toml")
        for p in [] if only_components else data.get("pipelines") or []:
            if not (ROOT / "pipelines" / p).is_dir():
                errors.append(f"pipeline '{p}' has no pipelines/{p}/")
        for e in errors:
            print(f"stack.yaml: {e}", file=sys.stderr)
        return 1 if errors else 0
    elif cmd == "flux":
        print("# GENERATED by scripts/stack.py flux from stack.yaml -- do not edit (gitignored)")
        print(yaml.safe_dump_all(flux_kustomizations(data), sort_keys=False, width=200), end="")
    elif cmd == "flux-names":
        names = ["flux-system", "platform", "platform-configs"]
        print("\n".join(names + [k["metadata"]["name"] for k in flux_kustomizations(data)]))
    else:
        print(f"unknown command: {cmd}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
