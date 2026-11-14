"""Probe the live CoinGlass global-assets page and discover its data endpoint.

This script intentionally performs read-only requests against the public page.
It does not log in, solve challenges, or write to CoinX storage.
"""

from __future__ import annotations

import argparse
import json
import re
from html import unescape
from pathlib import Path
from urllib.parse import urljoin

import requests


DEFAULT_URL = "https://coinglass.com/zh?category=crypto"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _candidate_urls(html: str, page_url: str) -> list[str]:
    """Return likely CoinGlass data URLs found in the initial document."""
    candidates: set[str] = set()
    for match in re.finditer(
        r"(?:https?:)?//[^\"'<>\\s]+|/(?:api|bapi|_next/data)/[^\"'<>\\s]+",
        html,
    ):
        value = unescape(match.group(0)).rstrip(".,);]}")
        if value.startswith("//"):
            value = f"https:{value}"
        try:
            absolute = urljoin(page_url, value)
        except ValueError:
            continue
        lowered = absolute.lower()
        if any(
            token in lowered
            for token in ("api", "bapi", "global", "asset", "overview", "market")
        ):
            candidates.add(absolute)
    return sorted(candidates)


def _script_urls(html: str, page_url: str) -> list[str]:
    """Return external script URLs from the initial document."""
    urls = set()
    for match in re.finditer(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        urls.add(urljoin(page_url, unescape(match.group(1))))
    return sorted(urls)


def _scan_script_sources(
    session: requests.Session,
    script_urls: list[str],
    page_url: str,
    timeout: float,
) -> tuple[list[str], list[dict[str, object]]]:
    """Fetch public page scripts and find literal API-like paths."""
    candidates: set[str] = set()
    scanned: list[dict[str, object]] = []
    search_terms = (
        "globalassets",
        "openinterest",
        "open_interest",
        "marketcap",
        "liquidation",
        "fundingrate",
    )
    for script_url in script_urls:
        try:
            response = session.get(script_url, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            scanned.append({"url": script_url, "status": "error", "error": str(exc)[:200]})
            continue

        source = response.text
        matches = set()
        contexts = []
        for match in re.finditer(
            r"(?:https?:)?//[^\"'`<>\\s]+|/(?:api|bapi|_next/data)/[^\"'`<>\\s]+",
            source,
        ):
            value = unescape(match.group(0)).rstrip(".,);]}")
            if value.startswith("//"):
                value = f"https:{value}"
            try:
                matches.add(urljoin(page_url, value))
            except ValueError:
                continue
        candidates.update(matches)
        lowered_source = source.lower()
        for term in search_terms:
            position = lowered_source.find(term)
            if position >= 0:
                contexts.append({
                    "term": term,
                    "context": source[max(0, position - 180):position + 420],
                })
        scanned.append({
            "url": script_url,
            "status": response.status_code,
            "matches": sorted(matches),
            "contexts": contexts,
        })
    return sorted(candidates), scanned


def _extract_next_data(html: str) -> object | None:
    match = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    try:
        return json.loads(unescape(match.group(1)))
    except json.JSONDecodeError:
        return None


def _write_snapshot(
    output: Path,
    html: str,
    candidates: list[str],
    script_urls: list[str],
    scanned_scripts: list[dict[str, object]],
    script_candidates: list[str],
    next_data: object | None,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "candidate_urls": candidates,
                "script_urls": script_urls,
                "script_candidates": script_candidates,
                "scanned_scripts": scanned_scripts,
                "has_next_data": next_data is not None,
                "next_data": next_data,
                "html_length": len(html),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--scan-scripts", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
    )

    response = session.get(args.url, timeout=args.timeout)
    response.raise_for_status()
    html = response.text
    candidates = _candidate_urls(html, response.url)
    script_urls = _script_urls(html, response.url)
    script_candidates: list[str] = []
    scanned_scripts: list[dict[str, object]] = []
    if args.scan_scripts:
        script_candidates, scanned_scripts = _scan_script_sources(
            session,
            script_urls,
            response.url,
            args.timeout,
        )
    next_data = _extract_next_data(html)

    print(json.dumps(
        {
            "status": response.status_code,
            "url": response.url,
            "content_type": response.headers.get("Content-Type"),
            "html_length": len(html),
            "has_next_data": next_data is not None,
            "candidate_count": len(candidates),
            "candidate_urls": candidates[:100],
            "script_count": len(script_urls),
            "script_urls": script_urls[:100],
            "script_candidate_count": len(script_candidates),
            "script_candidates": script_candidates[:200],
        },
        ensure_ascii=True,
        indent=2,
    ))

    if args.output:
        _write_snapshot(
            args.output,
            html,
            candidates,
            script_urls,
            scanned_scripts,
            script_candidates,
            next_data,
        )
        print(f"saved probe snapshot: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
