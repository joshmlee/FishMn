"""
Incremental survey updater for FishMn.

For each lake already in docs/data/surveys/*.json:
  - Fetches fresh data from the DNR API
  - Compares survey IDs against what we already have
  - Adds any new survey records (new dates / new gear rows)
  - Writes updated JSON files in-place

Also checks the county gazetteer for any new lakes added since last scrape,
fetching their surveys and inserting them into the right county file.

Usage:
  python update_surveys.py                  # update all counties
  python update_surveys.py --county Hennepin  # single county
  python update_surveys.py --new-lakes-only   # only scan for new lakes
"""

import json
import re
import time
import os
import argparse
import ast
from collections import defaultdict

try:
    import requests
except ImportError:
    raise SystemExit("Install requests: pip install requests")

SURVEYS_DIR = "docs/data/surveys"
LAKES_FILE  = "docs/data/lakes.json"
GAZETTEER_URL = "https://maps.dnr.state.mn.us/cgi-bin/gazetteer/gazetteer2.cgi"
DETAIL_URL    = "https://maps.dnr.state.mn.us/cgi-bin/lakefinder/detail.cgi"
DELAY = 0.35  # seconds between API calls

MN_COUNTIES = [
    (1,  "Aitkin"),    (2,  "Anoka"),      (3,  "Becker"),    (4,  "Beltrami"),
    (5,  "Benton"),    (6,  "Big Stone"),   (7,  "Blue Earth"),(8,  "Brown"),
    (9,  "Carlton"),   (10, "Carver"),      (11, "Cass"),      (12, "Chippewa"),
    (13, "Chisago"),   (14, "Clay"),        (15, "Clearwater"),(16, "Cook"),
    (17, "Cottonwood"),(18, "Crow Wing"),   (19, "Dakota"),    (20, "Dodge"),
    (21, "Douglas"),   (22, "Faribault"),   (23, "Fillmore"),  (24, "Freeborn"),
    (25, "Goodhue"),   (26, "Grant"),       (27, "Hennepin"),  (28, "Houston"),
    (29, "Hubbard"),   (30, "Isanti"),      (31, "Itasca"),    (32, "Jackson"),
    (33, "Kanabec"),   (34, "Kandiyohi"),   (35, "Kittson"),   (36, "Koochiching"),
    (37, "Lac qui Parle"),(38,"Lake"),      (39, "Lake of the Woods"),
    (40, "Le Sueur"),  (41, "Lincoln"),     (42, "Lyon"),      (43, "McLeod"),
    (44, "Mahnomen"),  (45, "Marshall"),    (46, "Martin"),    (47, "Meeker"),
    (48, "Mille Lacs"),(49, "Morrison"),    (50, "Mower"),     (51, "Murray"),
    (52, "Nicollet"),  (53, "Nobles"),      (54, "Norman"),    (55, "Olmsted"),
    (56, "Otter Tail"),(57, "Pennington"),  (58, "Pine"),      (59, "Pipestone"),
    (60, "Polk"),      (61, "Pope"),        (62, "Ramsey"),    (63, "Red Lake"),
    (64, "Redwood"),   (65, "Renville"),    (66, "Rice"),      (67, "Rock"),
    (68, "Roseau"),    (69, "St. Louis"),   (70, "Scott"),     (71, "Sherburne"),
    (72, "Sibley"),    (73, "Stearns"),     (74, "Steele"),    (75, "Stevens"),
    (76, "Swift"),     (77, "Todd"),        (78, "Traverse"),  (79, "Wabasha"),
    (80, "Wadena"),    (81, "Waseca"),      (82, "Washington"),(83, "Watonwan"),
    (84, "Wilkin"),    (85, "Winona"),      (86, "Wright"),    (87, "Yellow Medicine"),
]


def fetch_jsonp(url, params):
    params["callback"] = "cb"
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    text = r.text.strip()
    m = re.match(r"^\w+\((.*)\);?$", text, re.DOTALL)
    return json.loads(m.group(1) if m else text)


def get_lakes_in_county(county_id):
    data = fetch_jsonp(GAZETTEER_URL, {"type": "lake", "county": str(county_id)})
    if data.get("status") == "ERROR":
        return []
    return data.get("results", [])


def get_lake_surveys(dow):
    data = fetch_jsonp(DETAIL_URL, {"type": "lake_survey", "id": dow})
    if data.get("status") == "ERROR":
        return {}
    return data.get("result", {})


def compute_length_stats(fish_count):
    """fish_count is a list of [length, count] pairs."""
    if not fish_count:
        return None, None
    total = sum(c for _, c in fish_count)
    if total == 0:
        return None, None
    avg = sum(l * c for l, c in fish_count) / total
    return round(avg, 1), max(l for l, _ in fish_count)


def flatten_catch_rows(dow, surveys_raw):
    """Convert raw DNR API survey list to our flat record format."""
    rows = []
    for survey in surveys_raw:
        date = survey.get("surveyDate", "")
        lengths_map = survey.get("lengths", {})
        for catch in survey.get("fishCatchSummaries", []):
            sp = catch.get("species", "")
            ldata = lengths_map.get(sp, {})
            fish_count = ldata.get("fishCount", []) if isinstance(ldata, dict) else []
            avg_len, max_len = compute_length_stats(fish_count)
            rows.append({
                "survey_id": survey.get("surveyID", ""),
                "date": date,
                "type": survey.get("surveyType", ""),
                "species": sp,
                "gear": catch.get("gear", ""),
                "gear_count": catch.get("gearCount"),
                "total_catch": catch.get("totalCatch"),
                "cpue": catch.get("CPUE"),
                "avg_weight": catch.get("averageWeight"),
                "avg_length": avg_len,
                "max_length": max_len,
            })
    return rows


def county_filename(county_name):
    return os.path.join(SURVEYS_DIR, county_name.replace(" ", "_").replace(".", "") + ".json")


def load_county(county_name):
    path = county_filename(county_name)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_county(county_name, data):
    path = county_filename(county_name)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def update_county(county_id, county_name, new_lakes_only=False):
    county_data = load_county(county_name)
    existing_dows = set(county_data.keys())

    added_surveys = 0
    added_lakes = 0
    checked = 0

    # --- Check for new lakes via gazetteer ---
    print(f"  Fetching lake list for {county_name}...")
    try:
        gazetteer_lakes = get_lakes_in_county(county_id)
        time.sleep(DELAY)
    except Exception as e:
        print(f"  WARNING: Gazetteer fetch failed: {e}")
        gazetteer_lakes = []

    all_dows = {str(l["id"]): l.get("name", "") for l in gazetteer_lakes}
    new_dows = set(all_dows.keys()) - existing_dows
    if new_dows:
        print(f"  {len(new_dows)} new lakes found in gazetteer")

    # Determine which DOWs to process
    if new_lakes_only:
        dows_to_process = {d: all_dows[d] for d in new_dows}
    else:
        dows_to_process = {**{d: all_dows.get(d, f"DOW {d}") for d in existing_dows},
                           **{d: all_dows[d] for d in new_dows}}

    total = len(dows_to_process)
    for i, (dow, name) in enumerate(dows_to_process.items(), 1):
        if i % 50 == 0 or i == total:
            print(f"    [{i}/{total}] {county_name}...")

        try:
            result = get_lake_surveys(dow)
            time.sleep(DELAY)
        except Exception as e:
            print(f"    WARNING: {name} (DOW {dow}): {e}")
            time.sleep(1)
            continue

        checked += 1
        fresh_surveys = result.get("surveys", [])
        if not fresh_surveys:
            continue

        fresh_rows = flatten_catch_rows(dow, fresh_surveys)
        existing_rows = county_data.get(dow, [])

        # Find survey_ids already stored
        existing_ids = set(r.get("survey_id", "") for r in existing_rows)
        new_rows = [r for r in fresh_rows if r.get("survey_id", "") not in existing_ids]

        if new_rows:
            county_data[dow] = existing_rows + new_rows
            added_surveys += len(new_rows)
            if dow in new_dows:
                added_lakes += 1

    if added_surveys > 0 or added_lakes > 0:
        save_county(county_name, county_data)
        print(f"  Saved {county_name}: +{added_surveys} survey rows, +{added_lakes} new lakes")
    else:
        print(f"  {county_name}: no changes")

    return added_surveys, added_lakes, checked


def update_lakes_json():
    """Rebuild lakes.json from all county survey files."""
    print("Rebuilding lakes.json...")
    seen = {}
    for fname in sorted(os.listdir(SURVEYS_DIR)):
        if not fname.endswith(".json"):
            continue
        county = fname.replace(".json", "").replace("_", " ")
        with open(os.path.join(SURVEYS_DIR, fname)) as f:
            data = json.load(f)
        for dow in data:
            if dow not in seen:
                # Try to get name from first record's data or fall back
                seen[dow] = county
    # Merge with existing lakes.json to preserve names
    existing = {}
    if os.path.exists(LAKES_FILE):
        with open(LAKES_FILE) as f:
            for lake in json.load(f):
                existing[str(lake["id"])] = lake
    # Add any DOWs present in surveys but not in lakes.json
    new_entries = []
    for dow, county in seen.items():
        if dow not in existing:
            new_entries.append({"id": dow, "name": f"DOW {dow}", "county": county})
    if new_entries:
        lakes = list(existing.values()) + new_entries
        with open(LAKES_FILE, "w") as f:
            json.dump(lakes, f, separators=(",", ":"))
        print(f"  Added {len(new_entries)} new lakes to lakes.json")
    else:
        print("  lakes.json already up to date")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", help="Only update this county (exact name)")
    parser.add_argument("--new-lakes-only", action="store_true", help="Only fetch new lakes, skip existing DOWs")
    args = parser.parse_args()

    os.makedirs(SURVEYS_DIR, exist_ok=True)

    if args.county:
        counties = [(cid, cname) for cid, cname in MN_COUNTIES if cname == args.county]
        if not counties:
            raise SystemExit(f"County not found: {args.county}")
    else:
        counties = MN_COUNTIES

    total_surveys = 0
    total_lakes = 0
    total_checked = 0

    for county_id, county_name in counties:
        path = county_filename(county_name)
        if not os.path.exists(path) and args.new_lakes_only:
            print(f"Skipping {county_name} (no existing file, use full mode first)")
            continue
        print(f"\n[{county_id}/87] {county_name}...")
        s, l, c = update_county(county_id, county_name, new_lakes_only=args.new_lakes_only)
        total_surveys += s
        total_lakes += l
        total_checked += c

    update_lakes_json()

    print(f"\n=== Done ===")
    print(f"Lakes checked:        {total_checked}")
    print(f"New survey rows added: {total_surveys}")
    print(f"New lakes added:       {total_lakes}")
    print(f"\nNext step: run  python build_data.py  to rebuild the JSON files,")
    print(f"then commit and push to deploy.")
