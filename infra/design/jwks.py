#!/usr/bin/env python3
"""RSA public key (PEM, SubjectPublicKeyInfo) on stdin -> JWKS / RFC 7638 thumbprint on stdout.

  jwks.py kid          -> the key's RFC 7638 SHA-256 thumbprint (base64url), used as `kid`
  jwks.py jwks <kid>   -> {"keys": [{kty, alg: RS256, use: sig, kid, n, e}]}

Stdlib only (a tiny DER reader), so infra/design/install.sh needs nothing installed on the host.
"""

import base64
import hashlib
import json
import sys


def _tlv(buf: bytes, i: int) -> tuple[int, bytes, int]:
    tag, length = buf[i], buf[i + 1]
    i += 2
    if length & 0x80:
        n = length & 0x7F
        length = int.from_bytes(buf[i : i + n], "big")
        i += n
    return tag, buf[i : i + length], i + length


def rsa_numbers(pem: str) -> tuple[int, int]:
    body = "".join(line for line in pem.strip().splitlines() if not line.startswith("-----"))
    der = base64.b64decode(body)
    tag, spki, _ = _tlv(der, 0)
    assert tag == 0x30, "not a SubjectPublicKeyInfo"
    _, _, i = _tlv(spki, 0)  # AlgorithmIdentifier
    tag, bitstring, _ = _tlv(spki, i)
    assert tag == 0x03, "expected BIT STRING"
    tag, rsa, _ = _tlv(bitstring[1:], 0)  # skip the unused-bits byte
    assert tag == 0x30, "expected RSAPublicKey"
    _, n, i = _tlv(rsa, 0)
    _, e, _ = _tlv(rsa, i)
    return int.from_bytes(n, "big"), int.from_bytes(e, "big")


def b64url_uint(x: int) -> str:
    raw = x.to_bytes((x.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def main(argv: list[str]) -> int:
    n, e = rsa_numbers(sys.stdin.read())
    jwk = {"e": b64url_uint(e), "kty": "RSA", "n": b64url_uint(n)}
    if argv[:1] == ["kid"]:
        canonical = json.dumps(jwk, separators=(",", ":"), sort_keys=True).encode()
        print(base64.urlsafe_b64encode(hashlib.sha256(canonical).digest()).rstrip(b"=").decode())
    elif argv[:1] == ["jwks"] and len(argv) == 2:
        print(json.dumps({"keys": [{**jwk, "alg": "RS256", "use": "sig", "kid": argv[1]}]}))
    else:
        print(__doc__, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
