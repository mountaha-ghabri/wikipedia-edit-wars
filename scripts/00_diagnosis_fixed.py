"""
DIAGNOSTIC SCRIPT (FIXED) – Run locally on Windows with plain Python.
Automatically finds your bronze folder whether you run it from anywhere.

Usage:
    python 00_diagnose_FIXED.py
    -- or specify a path directly --
    python 00_diagnose_FIXED.py "C:\\Users\\HP\\Desktop\\sleeptrial\\data\\bronze\\wikipedia"
"""

import os
import sys
import json
import glob

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 – Resolve bronze directory
# ─────────────────────────────────────────────────────────────────────────────

# Accept path from command line, OR try to auto-detect
if len(sys.argv) > 1:
    BRONZE_DIR = sys.argv[1].strip('"').strip("'")
else:
    # Try common locations relative to this script's own folder
    script_dir = os.path.dirname(os.path.abspath(__file__))

    candidates = [
        # if script lives inside  sleeptrial/scripts/
        os.path.join(script_dir, "..", "data", "bronze", "wikipedia"),
        # if script lives inside  sleeptrial/
        os.path.join(script_dir, "data", "bronze", "wikipedia"),
        # absolute fall-back (original hard-coded path)
        r"C:\Users\HP\Desktop\sleeptrial\data\bronze\wikipedia",
        # second project you mentioned in the screenshot
        r"C:\Users\HP\Desktop\wikidit_project\data\raw\massive_batch",
        r"C:\Users\HP\Desktop\wikidit_project\data\raw\massive_batch",
    ]

    BRONZE_DIR = None
    for c in candidates:
        resolved = os.path.normpath(c)
        if os.path.isdir(resolved):
            BRONZE_DIR = resolved
            break

    if BRONZE_DIR is None:
        print("Could not auto-detect bronze directory.")
        print("Please pass the path as an argument, e.g.:")
        print(r'  python 00_diagnose_FIXED.py "C:\Users\HP\Desktop\sleeptrial\data\bronze\wikipedia"')
        sys.exit(1)

BRONZE_DIR = os.path.normpath(BRONZE_DIR)

print("=" * 70)
print("BRONZE DATA DIAGNOSTIC  (Windows-local version)")
print("=" * 70)
print(f"\nScanning: {BRONZE_DIR}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 – Find JSON files (also look one level up in case path is off by one)
# ─────────────────────────────────────────────────────────────────────────────
files = sorted(glob.glob(os.path.join(BRONZE_DIR, "*.json")))

if not files:
    # Try one directory up
    parent = os.path.dirname(BRONZE_DIR)
    files_up = sorted(glob.glob(os.path.join(parent, "*.json")))
    if files_up:
        print(f"\nNo JSON in that folder, but found {len(files_up)} JSON files one level up:")
        print(f"  {parent}")
        print("Using that folder instead.\n")
        BRONZE_DIR = parent
        files = files_up
    else:
        # List what IS in the folder so the user can see
        print(f"\nDirectory contents of '{BRONZE_DIR}':")
        try:
            for entry in os.listdir(BRONZE_DIR)[:30]:
                full = os.path.join(BRONZE_DIR, entry)
                size = os.path.getsize(full) if os.path.isfile(full) else 0
                kind = "DIR" if os.path.isdir(full) else "FILE"
                print(f"  {kind}  {entry}  ({size:,} bytes)")
        except Exception as e:
            print(f"  Could not list directory: {e}")
        print("\nNo JSON files found. Check the path and re-run.")
        sys.exit(1)

print(f"JSON files found: {len(files)}")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 – Inspect each file
# ─────────────────────────────────────────────────────────────────────────────
total_records = 0
total_ok      = 0
total_corrupt = 0
bot_count     = 0
anon_count    = 0
pages         = set()
users         = set()
format_counts = {"array_file": 0, "ndjson": 0, "empty": 0, "corrupt": 0}
problem_files = []

REQUIRED = {"page_title", "revid", "user", "timestamp"}

for fpath in files:
    fname = os.path.basename(fpath)
    fsize = os.path.getsize(fpath)

    if fsize == 0:
        format_counts["empty"] += 1
        continue

    # Read raw bytes first to detect encoding issues
    try:
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read().strip()
    except Exception as e:
        problem_files.append((fname, str(e)))
        format_counts["corrupt"] += 1
        total_corrupt += 1
        continue

    if not content:
        format_counts["empty"] += 1
        continue

    # ── Try to parse ─────────────────────────────────────────────────────────
    records   = []
    parsed_ok = False

    # Format A: the whole file is a JSON array  [{ }, { }, ...]
    if content.startswith("["):
        try:
            obj = json.loads(content)
            if isinstance(obj, list):
                records = [r for r in obj if isinstance(r, dict)]
                format_counts["array_file"] += 1
                parsed_ok = True
        except json.JSONDecodeError:
            pass

    # Format B: newline-delimited JSON (one object per line)
    if not parsed_ok:
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, list):
                    records.extend([r for r in obj if isinstance(r, dict)])
                elif isinstance(obj, dict):
                    records.append(obj)
                parsed_ok = True
            except json.JSONDecodeError:
                pass
        if parsed_ok:
            format_counts["ndjson"] += 1

    if not parsed_ok:
        problem_files.append((fname, "cannot parse as JSON"))
        format_counts["corrupt"] += 1
        total_corrupt += 1
        continue

    # ── Count valid records ───────────────────────────────────────────────────
    for r in records:
        total_records += 1
        if REQUIRED.issubset(r.keys()) and all(r.get(k) is not None for k in REQUIRED):
            total_ok += 1
            pages.add(r.get("page_title", ""))
            users.add(r.get("user", ""))
            if r.get("bot") is True:
                bot_count += 1
            if r.get("anon") is True:
                anon_count += 1

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 – Print report
# ─────────────────────────────────────────────────────────────────────────────
print("\n── File Format Breakdown ──")
for k, v in format_counts.items():
    print(f"  {k:15} : {v:>5}")

if problem_files:
    print(f"\n── Problematic Files ({len(problem_files)}) ──")
    for fname, reason in problem_files[:20]:
        print(f"  ⚠️  {fname} → {reason}")

print(f"\n── Record Counts ──")
print(f"  Total records parsed   : {total_records:>10,}")
print(f"  Valid records (4 keys) : {total_ok:>10,}")
print(f"  Corrupt files skipped  : {total_corrupt:>10,}")

print(f"\n── Content Summary ──")
print(f"  Unique pages           : {len(pages):>10,}")
print(f"  Unique users           : {len(users):>10,}")
print(f"  Bot edits              : {bot_count:>10,}  ({bot_count/max(total_ok,1)*100:.3f}%)")
print(f"  Anon edits             : {anon_count:>10,}  ({anon_count/max(total_ok,1)*100:.2f}%)")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 – Recommendations
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n── Recommendations ──")

if total_ok == 0:
    print("  ❌ Zero valid records. Something is still wrong with the path or files.")
elif total_ok < 10_000:
    print(f"  ⚠️  Only {total_ok:,} valid records. ML models will be weak.")
else:
    print(f"  ✅ {total_ok:,} valid records. ETL pipeline should work fine.")

if bot_count == 0:
    print("  ⚠️  No bot=True records found.")
    print("     Model 1 (bot detection) will only see class 0 → accuracy=1.0 is FAKE.")
    print("     The fixed ML script detects this and skips training gracefully.")
elif bot_count < 500:
    print(f"  ⚠️  Very few bot records ({bot_count:,}). Class-weighted RF is essential.")
else:
    print(f"  ✅ {bot_count:,} bot records ({bot_count/max(total_ok,1)*100:.2f}%).")

if total_corrupt > 0:
    print(f"\n  ℹ️  {total_corrupt} corrupt/unreadable files will be SKIPPED by Spark.")
    print("     That is normal for binary dump files — the fixed ETL handles them.")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6 – Show Docker mount path to use in Spark scripts
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n── Docker Path ──")
print(f"  Your local path  : {BRONZE_DIR}")
print(f"  Make sure docker-compose.yml mounts it as:")

# Convert Windows path to a posix-style docker volume hint
posix_hint = BRONZE_DIR.replace("\\", "/").replace("C:", "/c")
print(f"    volumes:")
print(f"      - {BRONZE_DIR}:/opt/spark/project/data/bronze/wikipedia")
print(f"  Then in Spark scripts use:")
print(f"    BRONZE_PATH = '/opt/spark/project/data/bronze/wikipedia/*.json'")

print("\n" + "=" * 70)
print("DIAGNOSTIC COMPLETE")
print("=" * 70)