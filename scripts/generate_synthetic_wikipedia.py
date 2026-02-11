"""
SYNTHETIC DATA TOP-UP GENERATOR
Adds ~2,200,000 more records to reach 10M+ total.
Saves directly into your existing bronze/wikipedia folder alongside real files.

Usage:
    python generate_topup.py
"""

import json
import random
import os
import sys
import glob
import time
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — edit these if needed
# ─────────────────────────────────────────────────────────────────────────────

CURRENT_COUNT   = 7_864_579          # from your diagnostic
TARGET_TOTAL    = 11_100_000         # slightly over 10M to be safe
RECORDS_NEEDED  = TARGET_TOTAL - CURRENT_COUNT   # = 2,235,421

BATCH_SIZE      = 50_000             # records per file (same as your existing files)

# Output folder — same folder as your existing bronze files
OUTPUT_DIR = r"C:\Users\HP\Desktop\sleeptrial\data\bronze\wikipedia"

# Starting file number — scan existing synthetic files to avoid overwriting
# ─────────────────────────────────────────────────────────────────────────────

PAGES = [
    "Donald_Trump", "Joe_Biden", "Barack_Obama", "Kamala_Harris",
    "Israel", "Palestine", "Ukraine", "Russia", "China",
    "United_States", "United_Kingdom", "European_Union",
    "Republican_Party_(United_States)", "Democratic_Party_(United_States)",
    "Climate_change", "Global_warming", "Artificial_intelligence",
    "Machine_learning", "ChatGPT", "GPT-4", "Large_language_model",
    "COVID-19_pandemic", "Vaccine", "Vaccination",
    "Python_(programming_language)", "JavaScript", "Rust_(programming_language)",
    "Quantum_computing", "Blockchain", "Bitcoin", "Cryptocurrency",
    "Taylor_Swift", "Beyoncé", "Drake_(musician)", "The_Beatles",
    "Marvel_Cinematic_Universe", "Star_Wars", "Game_of_Thrones",
    "Netflix", "YouTube", "TikTok", "Instagram",
    "World_War_II", "American_Civil_War", "French_Revolution",
    "Ancient_Egypt", "Roman_Empire", "Cold_War",
    "New_York_City", "London", "Paris", "Tokyo", "Beijing",
    "California", "Texas", "Florida", "Germany", "France",
    "Physics", "Chemistry", "Biology", "Mathematics",
    "Albert_Einstein", "Isaac_Newton", "Charles_Darwin",
    "DNA", "Evolution", "Big_Bang", "Black_hole",
    "FIFA_World_Cup", "Super_Bowl", "Olympics", "UEFA_Champions_League",
    "Lionel_Messi", "Cristiano_Ronaldo", "LeBron_James", "Tom_Brady",
]

# Extra pages so new batches feel slightly different
EXTRA_PAGES = [
    "Elon_Musk", "OpenAI", "Microsoft", "Apple_Inc", "Google",
    "Amazon_(company)", "Meta_Platforms", "Twitter", "Reddit",
    "SpaceX", "Tesla_Inc", "Nuclear_power", "Solar_energy",
    "Immigration", "Gun_control", "Abortion", "Supreme_Court",
    "NATO", "United_Nations", "World_Health_Organization",
    "Inflation", "Recession", "Stock_market", "Federal_Reserve",
    "World_War_I", "Vietnam_War", "Korean_War", "Gulf_War",
    "Harry_Potter", "Lord_of_the_Rings", "Avatar_(film)",
    "Michael_Jackson", "Elvis_Presley", "Bob_Dylan",
]
ALL_PAGES = PAGES + EXTRA_PAGES

BOT_USERS = [
    "Cydebot", "HBC_AIV_helperbot5", "SineBot", "DeltaQuadBot",
    "ClueBot_NG", "AnomieBOT", "BattyBot", "Citation_bot",
    "WPCleaner", "LargeDataBot", "FixerBot", "PatrolBot",
]

HUMAN_USERS = [
    "AcademicEditor2024", "WikiGnome_Pro", "FactChecker_99",
    "ProfessorX", "StudentEditor", "RetiredTeacher",
    "RandomContributor", "CasualEditor", "TopicEnthusiast",
    "GrammarNinja", "CitationHunter", "NPOVWatcher",
    "HistoryBuff_1984", "ScienceGeek_42", "TechWriter",
    "NightEditor", "DawnPatroller", "WikiSleuth",
    "ReliableSourcer", "DeadLinkFixer", "CategoryBot2",
]

ANON_USERS = [
    f"Anon_{a}.{b}.{c}.{d}"
    for a, b, c, d in [
        (192,168,1,1),(10,0,0,5),(172,16,0,3),(203,0,113,5),
        (198,51,100,2),(185,220,101,4),(77,88,8,8),(8,8,4,4),
    ]
] + ["Mobile_User", "Public_WiFi_User", "School_Network", "Library_PC"]

COMMENTS = [
    "Fixed typo", "Fixed spelling", "Grammar correction", "Copyedit",
    "Added citation", "Added reference", "Added source", "Citation needed",
    "Updated statistics", "Updated data", "Updated information",
    "Clarified wording", "Improved clarity", "Reworded for clarity",
    "Reverted vandalism", "Undid revision", "Restored previous version",
    "Removed unsourced content", "Removed spam",
    "Added section", "Expanded article", "Added details",
    "NPOV fix", "Neutrality", "Removed POV", "Wikified",
    "Style cleanup", "Formatting", "Fixed links",
    "Updated for 2024", "Updated for 2025", "Updated for 2026",
    "Recent developments", "Latest information",
    "rv", "Reverted edit by", "Undo", "Restore",
    "", ".", "/* Section */ ", "typo",
    "Adding short description", "Fixing dead link",
    "Merged from redirect", "Splitting section",
]

BOT_COMMENTS = [
    "Bot: Automated formatting", "Bot: Fixing links",
    "Bot: Updating citations", "Bot: Removing dead links",
    "Bot: Archiving URL", "Bot: Adding wikidata ID",
    "Bot: Fixing category", "Bot: Standardising dates",
]

TAGS_POOL = [
    [], [], [], [],          # most edits have no tags
    ["mobile edit"], ["mobile edit", "mobile web edit"],
    ["wikieditor"], ["visualeditor"],
    ["shortdesc helper"], ["AWB"],
]


def find_next_batch_number(output_dir: str) -> int:
    """Return the next available synthetic_batch_NNNNN number."""
    existing = glob.glob(os.path.join(output_dir, "synthetic_batch_*.json"))
    if not existing:
        return 0
    nums = []
    for f in existing:
        base = os.path.basename(f)
        try:
            nums.append(int(base.replace("synthetic_batch_", "").replace(".json", "")))
        except ValueError:
            pass
    return max(nums) + 1 if nums else 0


def generate_record(revid: int, now: datetime) -> dict:
    """Generate one realistic Wikipedia edit record."""

    # Page selection: top pages get more edits
    roll = random.random()
    if roll < 0.55:
        page = random.choice(ALL_PAGES[:20])      # hot pages
    elif roll < 0.80:
        page = random.choice(ALL_PAGES[:40])
    else:
        page = random.choice(ALL_PAGES)

    # User selection: ~15% bots, ~19% anon, rest human
    u_roll = random.random()
    if u_roll < 0.155:
        user    = random.choice(BOT_USERS)
        is_bot  = True
        is_anon = False
    elif u_roll < 0.345:
        user    = random.choice(ANON_USERS)
        is_bot  = False
        is_anon = True
    else:
        user    = random.choice(HUMAN_USERS)
        is_bot  = False
        is_anon = False

    # Timestamp: spread over last 3 years
    ts = now - timedelta(
        days=random.uniform(0, 1095),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    # Edit size
    if is_bot:
        size        = random.randint(800, 12000)
        size_change = random.randint(-80, 250)
        is_minor    = random.random() < 0.6
        comment     = random.choice(BOT_COMMENTS)
    elif is_anon:
        size        = random.randint(200, 40000)
        size_change = random.randint(-3000, 4000)
        is_minor    = random.random() < 0.15
        comment     = random.choice(COMMENTS)
    else:
        size        = random.randint(500, 55000)
        size_change = random.randint(-2500, 6000)
        is_minor    = random.random() < 0.28
        comment     = random.choice(COMMENTS)

    return {
        "page_title":    page,
        "page_id":       str(abs(hash(page)) % 10_000_000),
        "page_watchers": 0 if is_bot else random.randint(0, 600),
        "revid":         revid,
        "parentid":      revid - 1,
        "timestamp":     ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user":          user,
        "userid":        0 if is_anon else abs(hash(user)) % 1_000_000,
        "anon":          is_anon,
        "size":          max(0, size),
        "comment":       comment,
        "minor":         is_minor,
        "bot":           is_bot,
        "tags":          random.choice(TAGS_POOL),
        "collected_at":  now.isoformat(),
    }


def main():
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("SYNTHETIC TOP-UP GENERATOR")
    print("=" * 70)
    print(f"Current records  : {CURRENT_COUNT:>12,}")
    print(f"Target total     : {TARGET_TOTAL:>12,}")
    print(f"Records to add   : {RECORDS_NEEDED:>12,}")
    print(f"Batch size       : {BATCH_SIZE:>12,}")
    print(f"Batches to write : {(RECORDS_NEEDED + BATCH_SIZE - 1) // BATCH_SIZE:>12,}")
    print(f"Output folder    : {output_path.resolve()}")
    print("=" * 70)

    if not output_path.exists():
        print(f"ERROR: output folder does not exist: {output_path.resolve()}")
        sys.exit(1)

    start_batch_num = find_next_batch_number(str(output_path))
    print(f"First new batch  : synthetic_batch_{start_batch_num:05d}.json")
    print()

    # revid base: far above real Wikipedia revids and above existing synthetic ones
    base_revid = 9_100_000_000 + start_batch_num * BATCH_SIZE

    total_written = 0
    batch_index   = 0
    now           = datetime.utcnow()
    t_start       = time.time()

    while total_written < RECORDS_NEEDED:
        batch_n   = min(BATCH_SIZE, RECORDS_NEEDED - total_written)
        records   = []
        t_batch   = time.time()

        for i in range(batch_n):
            revid = base_revid + total_written + i
            records.append(generate_record(revid, now))

        fname    = f"synthetic_batch_{start_batch_num + batch_index:05d}.json"
        fpath    = output_path / fname

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)   # compact, no indent → faster + smaller

        total_written += batch_n
        batch_index   += 1
        elapsed        = time.time() - t_start
        rate           = total_written / elapsed if elapsed > 0 else 0
        eta            = (RECORDS_NEEDED - total_written) / rate / 60 if rate > 0 else 0
        pct            = total_written / RECORDS_NEEDED * 100

        print(f"  [{batch_index:>3}] {fname}  "
              f"{batch_n:>6,} recs  |  "
              f"total {total_written:>9,}/{RECORDS_NEEDED:,} ({pct:5.1f}%)  |  "
              f"rate {rate:>8,.0f}/s  |  ETA {eta:4.1f}m",
              flush=True)

    elapsed = time.time() - t_start
    new_total = CURRENT_COUNT + total_written

    print()
    print("=" * 70)
    print("✅  DONE")
    print("=" * 70)
    print(f"Records written  : {total_written:>12,}")
    print(f"New grand total  : {new_total:>12,}  (target was {TARGET_TOTAL:,})")
    print(f"Files created    : {batch_index:>12,}")
    print(f"Time taken       : {elapsed:>11.1f}s  ({elapsed/60:.1f} min)")
    print(f"Avg rate         : {total_written/elapsed:>10,.0f} rec/s")
    print()
    print("Next step — verify total:")
    print(f'  python 00_diagnose_FIXED.py "{output_path.resolve()}"')
    print()
    print("Then run the ETL pipeline as normal.")
    print("=" * 70)


if __name__ == "__main__":
    main()