"""Client (spec §9): every page renders through the gateway without a server error."""

import pytest

PAGES = ["/", "/landing/1", "/advertiser", "/advertiser/analytics", "/hot-ads"]


@pytest.mark.parametrize("path", PAGES)
def test_page_renders(http, path):
    r = http.get(path)
    assert r.status_code == 200, r.text[:500]
    assert "text/html" in r.headers["content-type"]
    text = r.text
    for marker in ("Internal Server Error", "Application error", "__next_error__"):
        assert marker not in text, f"{path}: {marker}"
