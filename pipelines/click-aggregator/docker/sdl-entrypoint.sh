#!/usr/bin/env bash
# Entry point of the JobManager and TaskManager containers (flinkConfiguration
# `kubernetes.entry.path`), wrapping the image's /docker-entrypoint.sh.
#
# Why: Flink's configuration has no env-var substitution, and the operator mounts the generated
# config read-only. The S3 credentials/endpoint come from the `aws-conn` secret as env vars
# (AWS_ENDPOINT_URL, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY), so we copy the config to
# a writable dir, append the s3.* keys (used by the flink-s3-fs-presto or -hadoop plugin, enabled
# via ENABLE_BUILT_IN_PLUGINS) and point FLINK_CONF_DIR at the copy. Secrets stay out of ConfigMaps.
set -euo pipefail

src=${FLINK_CONF_DIR:-/opt/flink/conf}
dst=/tmp/flink-conf
mkdir -p "$dst"
cp -rL "$src"/. "$dst"/

if [ -f "$dst/config.yaml" ]; then
  conf="$dst/config.yaml"
  quote() { printf "'%s'" "${1//\'/\'\'}"; }       # standard YAML (Flink >= 2.0)
else
  conf="$dst/flink-conf.yaml"
  quote() { printf '%s' "$1"; }                     # legacy flat format
fi

if [ -n "${AWS_ENDPOINT_URL:-}" ]; then
  {
    echo
    echo "s3.endpoint: $(quote "$AWS_ENDPOINT_URL")"
    echo "s3.endpoint.region: $(quote "${AWS_REGION:-us-east-1}")"
    echo "s3.path.style.access: true"
    echo "s3.access-key: $(quote "${AWS_ACCESS_KEY_ID:-test}")"
    echo "s3.secret-key: $(quote "${AWS_SECRET_ACCESS_KEY:-test}")"
  } >>"$conf"
  echo "sdl-entrypoint: S3 endpoint ${AWS_ENDPOINT_URL} (path-style) added to $conf"
else
  echo "sdl-entrypoint: WARNING AWS_ENDPOINT_URL not set; checkpoints/HA on s3:// will fail" >&2
fi

export FLINK_CONF_DIR="$dst"
exec /docker-entrypoint.sh "$@"
