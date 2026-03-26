#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.rollspel.nu"
FORUM_PATH = "/forums/varulvsspel.81/"
USER_AGENT = "VarulvScraperBot/4.0 (damogn på forumet)"
DATA_DIR = Path("data")
ARCHIVE_TAG = Path("archive.json")
ARCHIVE_NO_TAG = Path("archive_no_tag.json")
INDEX_FILE = DATA_DIR / "_sync_index.json"

DEFAULT_DELAY = 1.25
DEFAULT_TIMEOUT = 30
THREADS_PER_FORUM_PAGE = 20

# Sätt False om du absolut vill räkna första posten också.
# True är säkrare eftersom första posten ofta är SL/regler/exempelröster.
SKIP_FIRST_POST = True

CLEAN_PATTERNS = [
    re.compile(r'data-csrf="[^"]+"'),
    re.compile(r'name="_xfToken"\s+value="[^"]+"'),
    re.compile(r"csrf:\s*'[^']+'"),
    re.compile(r"\bnow:\s*\d+\b"),
    re.compile(r'data-lb-trigger="[^"]*?_xfUid[^"]*"'),
    re.compile(r'data-lb-id="[^"]*?_xfUid[^"]*"'),
    re.compile(r'js-lbImage-_xfUid[^"\s>]*'),
    re.compile(r'_xfUid-\d+-\d+'),
    re.compile(r'data-timestamp="\d+"'),
]

VOTE_WORD = r"(?:\bröst|\brorösostot)"
VOTE_START = re.compile(VOTE_WORD + r"\s*:\s*", re.I)
USER_TAG = re.compile(r'data-username="@([^"]+)"', re.I)
TRAILING_PUNCT = re.compile(r"[\s\.,!?;:]+$")

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def clean_vote_text(raw_html: str) -> str:
    s = re.sub(r"<[^>]+>", " ", raw_html)
    s = normalize_spaces(s)
    s = s.lstrip("@")
    s = TRAILING_PUNCT.sub("", s).strip()
    return s

def clean_html(text: str) -> str:
    out = []
    for line in text.splitlines():
        for pat in CLEAN_PATTERNS:
            line = pat.sub("", line)
        out.append(line.strip())
    return "\n".join(out)

def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(path: Path, obj, compact: bool = True) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    if compact:
        tmp.write_text(
            json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
    else:
        tmp.write_text(
            json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    tmp.replace(path)

def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

def pages_in_dir(d: Path):
    out = []
    for p in d.glob("page*.html"):
        m = re.fullmatch(r"page(\d+)\.html", p.name)
        if m:
            out.append((int(m.group(1)), p))
    out.sort(key=lambda x: x[0])
    return out

def local_last_page(thread_dir: Path) -> int:
    pages = pages_in_dir(thread_dir)
    return pages[-1][0] if pages else 0

def thread_title_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    t = soup.find("title")
    if not t or not t.text:
        return ""
    s = t.text.strip()
    for pfx in ("Nekromanti - ", "Varulv - "):
        if s.startswith(pfx):
            s = s[len(pfx):]
    return s.replace("| rollspel.nu", "").strip()

def build_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }
    )
    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def polite_sleep(delay: float) -> None:
    if delay > 0:
        time.sleep(delay)

def fetch_html(sess: requests.Session, url: str, timeout: int, delay: float) -> str:
    res = sess.get(url, timeout=timeout)
    polite_sleep(delay)
    if res.status_code != 200:
        raise RuntimeError(f"{url} gav status {res.status_code}")
    res.encoding = res.encoding or "utf-8"
    return res.text

def parse_forum_last_page_number(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    inp = soup.select_one("input.js-pageJumpPage")
    if inp and inp.has_attr("max"):
        try:
            return int(inp["max"])
        except ValueError:
            pass
    last = soup.select_one("a.pageNavSimple-el--last[href]")
    if last:
        m = re.search(r"/page-(\d+)", last.get("href", ""))
        if m:
            return int(m.group(1))
    return 1

def normalize_thread_slug_id(thread_href: str):
    m = re.search(r"/threads/([^/]+?\.\d+)", thread_href)
    return m.group(1) if m else None

def thread_base_url_from_slug(slug_id: str) -> str:
    return urljoin(BASE_URL, f"/threads/{slug_id}/")

def parse_forum_threads(html: str):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for item in soup.select("div.structItem.structItem--thread"):
        title_a = None
        for a in item.select("div.structItem-title a[href]"):
            href = a.get("href", "")
            if "/threads/" in href:
                title_a = a
                break
        if not title_a:
            continue

        slug_id = normalize_thread_slug_id(title_a.get("href", ""))
        if not slug_id:
            continue

        latest_time = item.select_one("time.structItem-latestDate[data-timestamp]")
        if not latest_time:
            latest_time = item.select_one("div.structItem-cell--latest time[data-timestamp]")

        try:
            latest_ts = int(latest_time["data-timestamp"]) if latest_time else 0
        except Exception:
            latest_ts = 0

        nums = []
        for a in item.select("span.structItem-pageJump a"):
            t = a.get_text(strip=True)
            if t.isdigit():
                nums.append(int(t))
        last_page_hint = max(nums) if nums else 1

        out.append(
            {
                "slug_id": slug_id,
                "title": title_a.get_text(" ", strip=True),
                "base_url": thread_base_url_from_slug(slug_id),
                "latest_ts": latest_ts,
                "last_page_hint": last_page_hint,
            }
        )
    return out

def crawl_forum(sess: requests.Session, timeout: int, delay: float, limit_threads: int):
    first_html = fetch_html(sess, urljoin(BASE_URL, FORUM_PATH), timeout, delay)
    forum_last_page = parse_forum_last_page_number(first_html)

    if limit_threads > 0:
        pages_needed = max(1, math.ceil(limit_threads / THREADS_PER_FORUM_PAGE))
        forum_pages = min(forum_last_page, pages_needed)
    else:
        forum_pages = forum_last_page

    threads = parse_forum_threads(first_html)
    for page in range(2, forum_pages + 1):
        try:
            html = fetch_html(sess, urljoin(BASE_URL, f"{FORUM_PATH}page-{page}"), timeout, delay)
            threads.extend(parse_forum_threads(html))
        except Exception as e:
            print(f"[VARNING] kunde inte läsa forumsida {page}: {e}", file=sys.stderr)

    uniq = {}
    for t in threads:
        prev = uniq.get(t["slug_id"])
        if not prev or t["latest_ts"] >= prev["latest_ts"]:
            uniq[t["slug_id"]] = t

    out = list(uniq.values())
    out.sort(key=lambda t: t["latest_ts"], reverse=True)
    return out[:limit_threads] if limit_threads > 0 else out

def thread_page_url(base_url: str, page_num: int) -> str:
    if page_num <= 1:
        return base_url
    if not base_url.endswith("/"):
        base_url += "/"
    return urljoin(base_url, f"page-{page_num}")

def verify_thread_identity(html: str, slug_id: str) -> bool:
    if slug_id in html:
        return True
    soup = BeautifulSoup(html, "html.parser")
    canon = soup.select_one("link[rel='canonical'][href]")
    return bool(canon and slug_id in canon.get("href", ""))

def write_if_changed(path: Path, html: str) -> bool:
    if path.exists():
        old = path.read_text(encoding="utf-8", errors="ignore")
        if clean_html(old) == clean_html(html):
            return False
    path.write_text(html, encoding="utf-8")
    return True

def sync_thread(sess, idx, t, timeout, delay):
    slug = t["slug_id"]
    thread_dir = DATA_DIR / slug
    ensure_dir(thread_dir)

    state = idx.setdefault("threads", {}).setdefault(slug, {})
    last_seen = state.get("latest_ts")

    x = local_last_page(thread_dir)
    y = max(1, int(t["last_page_hint"]))

    # Bara skip om tråden ser komplett ut lokalt
    if (
        last_seen is not None
        and t["latest_ts"]
        and int(last_seen) == int(t["latest_ts"])
        and x >= y
        and x > 0
    ):
        return False, "skip"

    if x == 0:
        page_range = list(range(1, y + 1))
        action = f"ny tråd, hämtar 1..{y}"
    elif y > x:
        page_range = [x] + list(range(x + 1, y + 1))
        action = f"nya sidor {x}->{y}"
    else:
        page_range = [x]
        action = f"kollar sista sidan {x}"

    wrote_any = False
    for page_num in page_range:
        html = fetch_html(sess, thread_page_url(t["base_url"], page_num), timeout, delay)
        if not verify_thread_identity(html, slug):
            raise RuntimeError(f"identitetstest misslyckades för {slug} page{page_num}")
        if write_if_changed(thread_dir / f"page{page_num}.html", html):
            wrote_any = True

    state["latest_ts"] = t["latest_ts"]
    state["last_page"] = max(x, y)
    return wrote_any, action

def split_fragments(html_fragment: str):
    s = re.sub(r"<br\s*/?>", "\n", html_fragment, flags=re.I)
    s = re.sub(r"</p\s*>", "\n", s, flags=re.I)
    s = re.sub(r"</li\s*>", "\n", s, flags=re.I)
    parts = [x.strip() for x in s.split("\n")]
    return [x for x in parts if x]

def make_thread_obj(slug_raw: str, name: str, players: set[str], votes: list[dict]):
    if not votes:
        return None
    votes.sort(key=lambda v: v.get("ts") or "")
    tss = [v["ts"] for v in votes if v.get("ts")]
    return {
        "slug": unquote(slug_raw),
        "slug_raw": slug_raw,
        "name": name or unquote(slug_raw),
        "players": sorted(players, key=lambda s: s.lower()),
        "range": {"min": min(tss) if tss else None, "max": max(tss) if tss else None},
        "votes": votes,
    }

def parse_thread(thread_dir: Path):
    pages = pages_in_dir(thread_dir)
    if not pages:
        return None, None

    slug_raw = thread_dir.name
    title = None
    authors = set()
    tag_targets = set()
    tag_votes = []
    no_tag_votes = []

    print(f"  parse: {slug_raw} ({len(pages)} sidor)")

    for page_num, page_path in pages:
        print(f"    läser {slug_raw}/page{page_num}.html")
        html = page_path.read_text(encoding="utf-8", errors="ignore")
        if title is None:
            title = thread_title_from_html(html)

        soup = BeautifulSoup(html, "html.parser")
        posts = soup.select("article[data-author]")

        for post_index, post in enumerate(posts):
            if SKIP_FIRST_POST and page_num == 1 and post_index == 0:
                continue

            from_user = (post.get("data-author") or "").strip()
            if not from_user:
                continue
            authors.add(from_user)

            pid = (post.get("id") or "").replace("js-post-", "").strip()
            t = post.select_one("time.u-dt")
            ts = (t.get("datetime") if t else "") or ""
            msg = post.select_one(".message-content")
            if not msg or not pid:
                continue

            for x in msg.select("blockquote, .bbCodeBlock--quote, .bbCodeBlock--unfurl, [data-unfurl='true']"):
                x.decompose()

            for frag in split_fragments(msg.decode_contents()):
                plain = normalize_spaces(re.sub(r"<[^>]+>", " ", frag))
                if not VOTE_START.search(plain):
                    continue

                tail_html = re.sub(rf"^[\s\S]*?{VOTE_WORD}\s*:\s*", "", frag, count=1, flags=re.I)
                tagged = USER_TAG.search(tail_html)

                if tagged:
                    to_user = (tagged.group(1) or "").lstrip("@").strip()
                    if to_user:
                        tag_targets.add(to_user)
                        tag_votes.append(
                            {
                                "from": from_user,
                                "to": to_user,
                                "ts": ts,
                                "post": pid,
                                "page": page_num,
                            }
                        )
                    continue

                raw_to = clean_vote_text(tail_html)
                if raw_to:
                    no_tag_votes.append(
                        {
                            "from": from_user,
                            "to": raw_to,
                            "ts": ts,
                            "post": pid,
                            "page": page_num,
                        }
                    )

    tagged_obj = make_thread_obj(slug_raw, title or unquote(slug_raw), authors | tag_targets, tag_votes)
    no_tag_obj = make_thread_obj(slug_raw, title or unquote(slug_raw), authors, no_tag_votes)
    return tagged_obj, no_tag_obj

def build_archive(by_slug: dict):
    threads = [{"slug": slug, "name": obj["name"]} for slug, obj in by_slug.items()]
    threads.sort(key=lambda x: x["name"].lower())
    return {"bySlug": by_slug, "threads": threads}

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-threads", type=int, default=0)
    args = ap.parse_args()

    ensure_dir(DATA_DIR)
    idx = load_json(INDEX_FILE, {"threads": {}})

    sess = build_session()

    print("SL: läser forumlistan...")
    try:
        forum_threads = crawl_forum(sess, DEFAULT_TIMEOUT, DEFAULT_DELAY, args.limit_threads)
    except Exception as e:
        print(f"[FEL] kunde inte läsa forumlistan: {e}", file=sys.stderr)
        return 2

    changed_slugs = []

    for i, t in enumerate(forum_threads, start=1):
        print(f"[{i}/{len(forum_threads)}] {t['title']}")
        try:
            changed, msg = sync_thread(sess, idx, t, DEFAULT_TIMEOUT, DEFAULT_DELAY)
            print(f"  {t['slug_id']}: {msg}")
            if changed:
                changed_slugs.append(t["slug_id"])
        except Exception as e:
            print(f"  {t['slug_id']}: FEL: {e}")

    save_json(INDEX_FILE, idx, compact=False)

    old_tag = load_json(ARCHIVE_TAG, {"bySlug": {}, "threads": []})
    old_no_tag = load_json(ARCHIVE_NO_TAG, {"bySlug": {}, "threads": []})

    by_slug_tag = old_tag.get("bySlug", {})
    by_slug_no_tag = old_no_tag.get("bySlug", {})

    if not ARCHIVE_TAG.exists() or not ARCHIVE_NO_TAG.exists():
        targets = sorted(
            [p.name for p in DATA_DIR.iterdir() if p.is_dir() and not p.name.startswith("_")],
            key=str.lower,
        )
    else:
        targets = sorted(set(changed_slugs), key=str.lower)

    print("SL: bygger archive.json och archive_no_tag.json...")
    print(f"SL: parse targets = {len(targets)}")

    for slug_raw in targets:
        thread_dir = DATA_DIR / slug_raw
        tagged_obj, no_tag_obj = parse_thread(thread_dir)
        slug = unquote(slug_raw)

        if tagged_obj:
            by_slug_tag[tagged_obj["slug"]] = tagged_obj
        else:
            by_slug_tag.pop(slug, None)

        if no_tag_obj:
            by_slug_no_tag[no_tag_obj["slug"]] = no_tag_obj
        else:
            by_slug_no_tag.pop(slug, None)

    save_json(ARCHIVE_TAG, build_archive(by_slug_tag), compact=True)
    save_json(ARCHIVE_NO_TAG, build_archive(by_slug_no_tag), compact=True)

    print(f"Klart: {ARCHIVE_TAG}")
    print(f"Klart: {ARCHIVE_NO_TAG}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
