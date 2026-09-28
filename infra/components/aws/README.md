# aws (Floci AWS emulator)

[Floci](https://github.com/floci-io/floci) **2.1.0**, installed from the quench community chart
**`floci` 0.2.18** (OCI `oci://ghcr.io/quenchworks/charts/floci`, Artifact Hub, MIT, no
account/token): a LocalStack-compatible AWS emulator (S3, SQS, SNS, DynamoDB, …) on port 4566.
Release `aws` (namespace `data`), `fullnameOverride: aws` so the Deployment/Service keep the name
`aws` (endpoint unchanged: `http://aws.data.svc.cluster.local:4566`). `floci.storage.mode:
persistent` + a 5Gi PVC (every write flushed to disk, buckets/objects survive pod restarts). 100m /
256Mi request, 1Gi limit. `apps/aws-conn` comes from a second release, `aws-glue` (`bedag/raw`
2.0.2) — the chart itself has no `extraObjects`.

**Image pinning:** the chart's default `image.repository: ghcr.io/quenchworks/images/floci` is
already pinned by **digest** (`sha256:4b39fd37…`), with `tag: "2.1.0"` kept only as a human label —
this matches today's Floci version, so `values/small.yaml` doesn't override `image:` at all; the
chart's own pin **is** the pin. (Not the same registry as the previous `floci/floci:2.1.0` Docker
Hub image, but the same upstream release.)

**Network policy:** the chart defaults to same-namespace-only ingress; since clients live in `apps`
and Floci runs in `data`, `values/small.yaml` sets `networkPolicy.allowExternal: true`.

One instance per design (`install.sh small [aws]`; any other instance name fails fast).

## Profiles

| Profile | What                        |
| ------- | --------------------------- |
| `small` | 1 pod + PVC (the only one — an emulator has no meaningful HA; the chart's `replicaCount` must stay 1 with persistent storage since each replica would hold its own state) |

## Connection contract — Secret `apps/aws-conn`

| Key                 | Env (with `connections: [aws]`) | Value                                     |
| ------------------- | ------------------------------- | ----------------------------------------- |
| `ENDPOINT_URL`      | `AWS_ENDPOINT_URL`              | `http://aws.data.svc.cluster.local:4566`  |
| `REGION`            | `AWS_REGION`                    | `us-east-1`                               |
| `ACCESS_KEY_ID`     | `AWS_ACCESS_KEY_ID`             | `test` (dummy)                            |
| `SECRET_ACCESS_KEY` | `AWS_SECRET_ACCESS_KEY`         | `test` (dummy)                            |

The env names are exactly what boto3 / the AWS CLI / AWS SDKs read, so `connections: [aws]` is
enough for most clients. **Use path-style S3 addressing** (`http://aws…:4566/<bucket>/<key>`):
virtual-host style would need `<bucket>.aws.data.svc.cluster.local` DNS.

- boto3: `boto3.client("s3", config=Config(s3={"addressing_style": "path"}))`
- AWS CLI: `aws configure set default.s3.addressing_style path`
- Flink (`flink-s3-fs-presto` or `flink-s3-fs-hadoop` built-in plugin):
  `s3.endpoint: <ENDPOINT_URL>`, `s3.path.style.access: "true"`, `s3.access-key`, `s3.secret-key`
  (see `../flink/README.md`).

Buckets/queues are design-specific: create them with a Job in `infra/design/`
(`envFrom: [{secretRef: {name: aws-conn}, prefix: AWS_}]` + `amazon/aws-cli`).

## Smoke test

`smoke.sh` (from namespace `apps`, `amazon/aws-cli:2.37.4`, the `aws-conn` values): S3 bucket
create + put/get with path-style addressing (content compared), a 20 MiB **multipart** upload
(5 MiB parts; ETag `…-4`) downloaded and compared byte-for-byte, SQS send/receive.

## Experiments

- Delete the Floci pod while a Flink job checkpoints to it: checkpoints fail until it's back
  (tolerable failures), the job keeps running; data on the PVC survives.
- Check what your code does when S3 is slow: NetworkChaos `delay` on `app.kubernetes.io/name=aws`.
