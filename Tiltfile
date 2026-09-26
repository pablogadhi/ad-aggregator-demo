# -*- mode: Python -*-
# Builds and deploys everything the design owns (services, pipelines, client) onto the cluster
# created by `make up`. Infra components are installed by `make up`, not by Tilt.
#
#   make dev   -> tilt up  (UI at http://localhost:10350, rebuilds on change)
#   make ci    -> tilt ci  (headless: exits 0 once everything is healthy AND the e2e tests pass)

allow_k8s_contexts('kind-sdl')
update_settings(max_parallel_updates=4, k8s_upsert_timeout_secs=300)

stack = read_yaml('stack.yaml')
CHART = 'infra/charts/app'
deployed = []

def app_resource(name, context, values, dockerfile=None, build_args={}, ignore=[], labels=[]):
    """One image + one release of the generic chart. The image name equals the release name;
    Tilt pushes it to the ctlptl registry (localhost:5005) and rewrites the Deployment."""
    docker_build(name, context, dockerfile=dockerfile, build_args=build_args, ignore=ignore)
    k8s_yaml(helm(CHART, name=name, namespace='apps', values=[values], set=['image.repository=' + name]))
    k8s_resource(name, labels=labels)
    deployed.append(name)

# --- services: services/<name>, built with the shared services/Dockerfile -----------------
for svc in stack.get('services') or []:
    app_resource(
        svc,
        context='services',
        dockerfile='services/Dockerfile',
        build_args={'SERVICE': svc},
        values='services/%s/deploy/values.yaml' % svc,
        ignore=['**/.venv', '**/__pycache__', '**/tests', '_template'],
        labels=['services'],
    )

# --- pipelines: each pipelines/<name>/Tiltfile defines its own build + deploy ---------------
# Convention: it must also define a resource named `<name>-running` that succeeds once the job is
# actually running (pods alone don't mean the job was submitted); e2e waits for it.
for p in stack.get('pipelines') or []:
    include('pipelines/%s/Tiltfile' % p)
    deployed.append('%s-running' % p)

# --- client ------------------------------------------------------------------------------
if stack.get('client'):
    app_resource(
        'client',
        context='client',
        values='client/deploy/values.yaml',
        ignore=['node_modules', '.next'],
        labels=['client'],
    )

# --- acceptance tests (tests/e2e) against the gateway, after everything is up --------------
local_resource(
    'e2e',
    # exit code 5 = no tests collected (fresh design before the integrator writes them)
    cmd='cd tests/e2e && { uv run --quiet pytest -q; rc=$?; [ $rc -eq 5 ] && echo "no e2e tests yet" && rc=0; exit $rc; }',
    env={'SDL_GATEWAY_URL': os.getenv('SDL_GATEWAY_URL', 'http://localhost:8080')},
    resource_deps=deployed,
    trigger_mode=TRIGGER_MODE_MANUAL,   # dev: click to run; ci: runs once automatically
    auto_init=True,
    labels=['tests'],
)
