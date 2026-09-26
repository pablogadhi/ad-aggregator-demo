#!/usr/bin/env bash
# Proves real behaviour, not just "pods Running" — from namespace apps, with the aws-conn values:
#   1. S3 with path-style addressing: create bucket, put/get an object (content compared)
#   2. S3 multipart upload (20 MiB via `aws s3 cp`, 5 MiB parts) round-trips byte-for-byte
#   3. SQS send/receive
source "$(dirname "$0")/../../../scripts/lib.sh"
AWS_CLI_IMAGE=amazon/aws-cli:2.37.4

get() { kc -n apps get secret aws-conn -o jsonpath="{.data.$1}" | base64 -d; }
for k in ENDPOINT_URL REGION ACCESS_KEY_ID SECRET_ACCESS_KEY; do [ -n "$(get $k)" ] || die "aws-conn: missing key $k"; done
token="smoke-$(date +%s)"

script="set -e
export AWS_ENDPOINT_URL=$(get ENDPOINT_URL) AWS_REGION=$(get REGION) AWS_ACCESS_KEY_ID=$(get ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=$(get SECRET_ACCESS_KEY)
aws configure set default.s3.addressing_style path
aws configure set default.s3.multipart_threshold 8MB
aws configure set default.s3.multipart_chunksize 5MB
aws s3api head-bucket --bucket sdl-smoke >/dev/null 2>&1 || aws s3api create-bucket --bucket sdl-smoke >/dev/null
echo $token > /tmp/small
aws s3api put-object --bucket sdl-smoke --key $token/small --body /tmp/small >/dev/null
aws s3api get-object --bucket sdl-smoke --key $token/small /tmp/small.out >/dev/null
[ \"\$(md5sum < /tmp/small)\" = \"\$(md5sum < /tmp/small.out)\" ] && echo put-get=ok
head -c 20971520 /dev/urandom > /tmp/big
aws s3 cp --only-show-errors /tmp/big s3://sdl-smoke/$token/big
aws s3 cp --only-show-errors s3://sdl-smoke/$token/big /tmp/big.out
[ \"\$(md5sum < /tmp/big)\" = \"\$(md5sum < /tmp/big.out)\" ] && echo multipart=ok
aws s3api head-object --bucket sdl-smoke --key $token/big --query ETag --output text
aws s3 rm --only-show-errors --recursive s3://sdl-smoke/$token
q=\$(aws sqs create-queue --queue-name sdl-smoke-$token --query QueueUrl --output text)
aws sqs send-message --queue-url \$q --message-body $token >/dev/null
echo sqs=\$(aws sqs receive-message --queue-url \$q --wait-time-seconds 5 --query 'Messages[0].Body' --output text)
aws sqs delete-queue --queue-url \$q"
log "aws: S3 (path-style, multipart) + SQS from namespace apps"
out=$(run_once apps "$AWS_CLI_IMAGE" sh -c "$script") || die "aws smoke failed: $out"
echo "$out" | grep -q '^put-get=ok$' || die "aws: S3 put/get failed: $out"
ok "S3 put/get (path-style)"
echo "$out" | grep -q '^multipart=ok$' || die "aws: S3 multipart upload failed: $out"
etag=$(echo "$out" | grep -o '"[0-9a-f]*-[0-9]*"' || true)
[ -n "$etag" ] || die "aws: 20 MiB upload was not multipart (ETag without part count): $out"
ok "S3 multipart upload round-trip (ETag $etag)"
echo "$out" | grep -q "^sqs=$token$" || die "aws: SQS send/receive failed: $out"
ok "SQS send/receive"
ok "aws smoke passed"
