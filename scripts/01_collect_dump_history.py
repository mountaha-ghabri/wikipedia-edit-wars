"""
SCRIPT 1B: Wikimedia Content History XML → Bronze JSON

Goal
----
- Download MediaWiki Content History XML dumps (mediawiki_content_history)
  for a given wiki (e.g. enwiki).
- Stream-parse the XML (compressed .xml.bz2) to extract revision metadata.
- Write JSON in the SAME bronze schema as 01_collect_data.py into:
    /opt/spark/project/data/bronze/wikipedia/dump_bronze_part_*.json

This lets 02_bronze_to_silver.py treat API edits and dump edits uniformly,
and easily reach 10M+ REAL revisions.
"""

import bz2
import datetime as dt
import json
import re
from pathlib import Path
from typing import Iterable, Dict
import xml.etree.ElementTree as ET

import requests


CONFIG_PATH = "/opt/spark/project/config/api_config.json"
DUMPS_DIR = Path("/opt/spark/project/data/bronze/dumps")
BRONZE_OUT_DIR = Path("/opt/spark/project/data/bronze/wikipedia")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def find_latest_content_history_sha(wiki: str, base_url: str) -> str:
    """
    Try recent months for a SHA256SUMS file for mediawiki_content_history.
    Pattern:
      {base_url}/{wiki}/{date}/xml/bzip2/SHA256SUMS
    where date is YYYY-MM-01.
    """
    if not base_url.endswith("/"):
        base_url += "/"

    today = dt.date.today().replace(day=1)
    for i in range(0, 12):  # up to last 12 months
        month = today.month - i
        year = today.year
        while month <= 0:
            month += 12
            year -= 1
        date_str = f"{year}-{month:02d}-01"
        sha_url = f"{base_url}{wiki}/{date_str}/xml/bzip2/SHA256SUMS"
        print(f"   🔎 Trying SHA256SUMS: {sha_url}")
        resp = requests.get(sha_url, timeout=60)
        if resp.status_code == 200:
            print(f"   ✅ Found SHA256SUMS for {date_str}")
            return date_str, resp.text
    raise SystemExit(f"Could not find SHA256SUMS for {wiki} under {base_url} (last 12 months).")


def parse_sha256sums(text: str) -> Iterable[str]:
    """
    Given contents of SHA256SUMS, yield relative paths to .xml.bz2 files.
    Lines look like:
        <sha256>  enwiki-2026-01-01-p123p456.xml.bz2
    """
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        rel_path = parts[-1]
        if rel_path.endswith(".xml.bz2"):
            yield rel_path


def download_with_resume(url: str, dest: Path, chunk_size: int = 1024 * 1024) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = dest.stat().st_size if dest.exists() else 0
    headers = {}
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"
        print(f"   ↻ Resuming download at {existing / (1024**2):.2f} MB")

    with requests.get(url, stream=True, headers=headers, timeout=60) as r:
        if r.status_code == 416:
            # Local file larger/newer than server thinks; start from scratch.
            print("   ⚠️  Got HTTP 416 for ranged request, deleting local file and restarting download...")
            if dest.exists():
                dest.unlink()
            existing = 0
            headers.pop("Range", None)
            with requests.get(url, stream=True, timeout=60) as r2:
                r2.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r2.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
            return dest

        r.raise_for_status()
        mode = "ab" if existing > 0 else "wb"
        with open(dest, mode) as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    return dest


def strip_ns(tag: str) -> str:
    """Remove XML namespace from tag."""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def iter_revisions_from_xml(path: Path) -> Iterable[Dict]:
    """
    Stream parse a MediaWiki export XML (.xml.bz2) and yield revision dicts.
    """
    print(f"   📖 Parsing XML: {path}")
    with bz2.open(path, "rt", encoding="utf-8", errors="ignore") as f:
        context = ET.iterparse(f, events=("start", "end"))
        page_title = None
        page_id = None
        for event, elem in context:
            tag = strip_ns(elem.tag)

            if event == "start" and tag == "page":
                page_title = None
                page_id = None

            if event == "end":
                if tag == "title":
                    page_title = (elem.text or "").strip()
                elif tag == "id" and page_id is None:
                    # First <id> in a <page> is page_id
                    try:
                        page_id = int((elem.text or "0").strip())
                    except ValueError:
                        page_id = 0
                elif tag == "revision":
                    # Extract revision fields
                    rev = {"page_title": page_title, "page_id": str(page_id)}
                    rev_id = 0
                    parent_id = 0
                    timestamp = ""
                    user = ""
                    user_id = 0
                    anon = False
                    comment = ""
                    minor = False
                    text_len = 0

                    for child in elem:
                        ctag = strip_ns(child.tag)
                        if ctag == "id":
                            try:
                                rev_id = int((child.text or "0").strip())
                            except ValueError:
                                rev_id = 0
                        elif ctag == "parentid":
                            try:
                                parent_id = int((child.text or "0").strip())
                            except ValueError:
                                parent_id = 0
                        elif ctag == "timestamp":
                            timestamp = (child.text or "").strip()
                        elif ctag == "contributor":
                            for c2 in child:
                                c2tag = strip_ns(c2.tag)
                                if c2tag in ("username", "ip"):
                                    user = (c2.text or "").strip()
                                    if c2tag == "ip":
                                        anon = True
                                elif c2tag == "id":
                                    try:
                                        user_id = int((c2.text or "0").strip())
                                    except ValueError:
                                        user_id = 0
                        elif ctag == "comment":
                            comment = (child.text or "").strip()
                        elif ctag == "minor":
                            minor = True
                        elif ctag == "text":
                            text_len = len(child.text or "")

                    rev.update(
                        {
                            "page_watchers": 0,
                            "revid": rev_id,
                            "parentid": parent_id,
                            "timestamp": timestamp,
                            "user": user or "Unknown",
                            "userid": user_id,
                            "anon": anon,
                            "size": text_len,
                            "comment": comment,
                            "minor": minor,
                            "bot": False,
                            "tags": [],
                            "collected_at": None,
                        }
                    )
                    yield rev
                    elem.clear()
                elif tag == "page":
                    elem.clear()


def write_json_batch(records, batch_idx: int):
    BRONZE_OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = BRONZE_OUT_DIR / f"dump_bronze_part_{batch_idx:05d}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
    print(f"   💾 Wrote batch {batch_idx} with {len(records):,} records -> {path.name}")


def main():
    cfg = load_config()
    dc = cfg.get("data_collection", {})

    dump_base_url = dc.get("dump_base_url", "https://dumps.wikimedia.org/other/mediawiki_content_history/")
    dump_wiki = dc.get("dump_wiki", "enwiki")
    dump_limit = int(dc.get("dump_limit") or 0)
    if dump_limit <= 0:
        dump_limit = 10_000_000

    print("=" * 80)
    print("WIKIMEDIA CONTENT HISTORY DUMP → BRONZE JSON")
    print("=" * 80)
    print(f"Wiki        : {dump_wiki}")
    print(f"Base URL    : {dump_base_url}")
    print(f"Target rows : {dump_limit:,}")

    print("\n🔎 Locating latest content-history SHA256SUMS...")
    date_str, sha_text = find_latest_content_history_sha(dump_wiki, dump_base_url)
    base_files_url = f"{dump_base_url.rstrip('/')}/{dump_wiki}/{date_str}/xml/bzip2/"
    print(f"✅ Using snapshot {date_str}")
    print(f"   Files base URL: {base_files_url}")

    rel_paths = list(parse_sha256sums(sha_text))
    if not rel_paths:
        raise SystemExit("No .xml.bz2 files listed in SHA256SUMS.")

    print(f"   Total dump files listed: {len(rel_paths)}")

    total = 0
    batch = []
    batch_idx = 0

    # To keep runtime reasonable, we don't need all files; we stop once we hit dump_limit.
    for rel in rel_paths:
        if total >= dump_limit:
            break
        file_name = rel.split("/")[-1]
        url = base_files_url + file_name
        dest = DUMPS_DIR / file_name

        print(f"\n⬇️  Downloading dump file: {url}")
        download_with_resume(url, dest)
        print(f"   ✅ Downloaded {dest.name} ({dest.stat().st_size / (1024**2):.2f} MB)")

        for rev in iter_revisions_from_xml(dest):
            batch.append(rev)
            total += 1
            if len(batch) >= 50_000:
                write_json_batch(batch, batch_idx)
                batch_idx += 1
                batch = []
            if total >= dump_limit:
                break

    if batch:
        write_json_batch(batch, batch_idx)

    print("\n" + "=" * 80)
    print("✅ DUMP COLLECTION COMPLETE")
    print("=" * 80)
    print(f"Total dump-based records: {total:,}")
    print(f"Output directory        : {BRONZE_OUT_DIR}")
    print("=" * 80)


if __name__ == "__main__":
    main()


